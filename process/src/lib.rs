//! The process library: a process is an object at rest in the manager's table,
//! given one thing at a time, and it gives.
//!
//! Nothing here waits. What waits, the queue, the sockets and the disk, is the
//! world the manager is handed: the Queue it takes from and the Tracing it
//! writes every birth, gift, taking and end into.

#![no_std]
extern crate alloc;

use alloc::vec::Vec;
use core::marker::PhantomData;

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Root(pub i64);

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Path(pub Vec<i64>);

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Identity {
    pub root: Root,
    pub path: Path,
}

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct Living<P> {
    pub identity: Identity,
    pub process: P,
}

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct Given<T> {
    pub identity: Identity,
    pub taken: T,
}

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct Gave<G> {
    pub identity: Identity,
    pub gift: G,
}

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct Ended<O> {
    pub identity: Identity,
    pub outcome: O,
}

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum Trace<P, G, T, O> {
    Born(Living<P>),
    Gave(Gave<G>),
    Took(Given<T>),
    Ended(Ended<O>),
}

/// A process: it begins once, takes each thing that comes for it after, and
/// yields its outcome when it ends. None is rest.
pub trait Processable {
    type Taken;
    type Gift;
    type Outcome;
    fn begin<N: Giving<Self::Gift>>(&mut self, giving: N) -> Option<Self::Outcome>;
    fn take<N: Giving<Self::Gift>>(&mut self, taken: Self::Taken, giving: N) -> Option<Self::Outcome>;
}

/// What a process is handed to give with. To give is to move.
pub trait Giving<Gift> {
    fn give<N: Givable + Into<Gift>>(&mut self, gift: N);
}

/// A thing that may leave a process; Returned is what comes back for it.
pub trait Givable {
    type Returned;
}

/// The outcome of a process ended with its owner.
pub trait Unwindable {
    fn unwound() -> Self;
}

/// What the queue yields: a process to be born, or a thing for a living one.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Arrival<P, T> {
    Born { owner: Option<Identity>, process: P },
    Given(Given<T>),
}

/// The manager's one queue; next waits for the next thing, and None ends the run.
pub trait Queue<P, T> {
    fn next(&mut self) -> Option<Arrival<P, T>>;
}

/// Where the trace goes: every gift is carried from here, every end announced.
pub trait Tracing<P, G, T, O> {
    fn trace(&mut self, trace: Trace<P, G, T, O>);
}

struct Entry<P> {
    living: Living<P>,
    born: i64,
}

pub struct ProcessManager<P> {
    table: Vec<Entry<P>>,
    roots: i64,
}

impl<P> Default for ProcessManager<P> {
    fn default() -> Self {
        Self { table: Vec::new(), roots: 0 }
    }
}

struct Giver<'a, W, P, T, O> {
    identity: &'a Identity,
    world: &'a mut W,
    traced: PhantomData<fn() -> (P, T, O)>,
}

impl<P, G, T, O, W> Giving<G> for Giver<'_, W, P, T, O>
where
    W: Tracing<P, G, T, O>,
{
    fn give<N: Givable + Into<G>>(&mut self, gift: N) {
        self.world.trace(Trace::Gave(Gave { identity: self.identity.clone(), gift: gift.into() }));
    }
}

impl<P, G, T, O> ProcessManager<P>
where
    P: Processable<Taken = T, Gift = G, Outcome = O> + Clone,
    T: Clone,
    O: Unwindable,
{
    pub fn run<Q, W>(&mut self, queue: &mut Q, world: &mut W)
    where
        Q: Queue<P, T>,
        W: Tracing<P, G, T, O>,
    {
        while let Some(arrival) = queue.next() {
            match arrival {
                Arrival::Born { owner, process } => self.bear(owner, process, world),
                Arrival::Given(given) => self.hand(given, world),
            }
        }
        self.end_all(world);
    }

    pub fn bear<W: Tracing<P, G, T, O>>(&mut self, owner: Option<Identity>, process: P, world: &mut W) {
        let identity = match owner {
            None => {
                self.roots += 1;
                Identity { root: Root(self.roots), path: Path(Vec::new()) }
            }
            Some(owner) => match self.table.iter_mut().find(|entry| entry.living.identity == owner) {
                Some(entry) => {
                    entry.born += 1;
                    let mut path = owner.path.0.clone();
                    path.push(entry.born);
                    Identity { root: owner.root, path: Path(path) }
                }
                None => return,
            },
        };
        world.trace(Trace::Born(Living { identity: identity.clone(), process: process.clone() }));
        let mut living = Living { identity, process };
        let outcome = living.process.begin(Giver { identity: &living.identity, world: &mut *world, traced: PhantomData });
        let identity = living.identity.clone();
        self.table.push(Entry { living, born: 0 });
        if let Some(outcome) = outcome {
            self.end(identity, outcome, world);
        }
    }

    pub fn hand<W: Tracing<P, G, T, O>>(&mut self, given: Given<T>, world: &mut W) {
        let Some(entry) = self.table.iter_mut().find(|entry| entry.living.identity == given.identity) else {
            return;
        };
        world.trace(Trace::Took(given.clone()));
        let living = &mut entry.living;
        if let Some(outcome) = living.process.take(given.taken, Giver { identity: &living.identity, world: &mut *world, traced: PhantomData }) {
            self.end(given.identity, outcome, world);
        }
    }

    /// A reverse unwind: what the process owns ends first, the last born first.
    pub fn end<W: Tracing<P, G, T, O>>(&mut self, identity: Identity, outcome: O, world: &mut W) {
        let owned: Vec<Identity> = self.owned_by(&identity).collect();
        for owned in owned.into_iter().rev() {
            self.end(owned, O::unwound(), world);
        }
        self.table.retain(|entry| entry.living.identity != identity);
        world.trace(Trace::Ended(Ended { identity, outcome }));
    }

    /// Every living process, ended the last born first.
    pub fn end_all<W: Tracing<P, G, T, O>>(&mut self, world: &mut W) {
        let roots: Vec<Identity> = self
            .table
            .iter()
            .map(|entry| entry.living.identity.clone())
            .filter(|identity| identity.path.0.is_empty())
            .collect();
        for root in roots.into_iter().rev() {
            self.end(root, O::unwound(), world);
        }
    }

    fn owned_by<'a>(&'a self, owner: &'a Identity) -> impl Iterator<Item = Identity> + 'a {
        self.table.iter().map(|entry| &entry.living.identity).filter(move |identity| {
            identity.root == owner.root
                && identity.path.0.len() == owner.path.0.len() + 1
                && identity.path.0.starts_with(&owner.path.0)
        }).cloned()
    }
}
