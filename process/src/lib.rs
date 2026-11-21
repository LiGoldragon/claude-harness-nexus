//! The process library: a process is an object at rest in the manager's table,
//! given one thing at a time, and it gives.
//!
//! Nothing here waits. What waits, the queue, the sockets and the disk, is the
//! world the manager is handed: the queue it iterates and the Tracing it
//! writes every birth, gift, taking and end into.

#![no_std]
extern crate alloc;

use alloc::vec::Vec;
use core::marker::PhantomData;

#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Root(pub i64);

#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Path(pub Vec<i64>);

#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Identity {
    pub root: Root,
    pub path: Path,
}

#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct Living<P> {
    pub identity: Identity,
    pub process: P,
}

#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct Given<T> {
    pub identity: Identity,
    pub taken: T,
}

#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct Gave<G> {
    pub identity: Identity,
    pub gift: G,
}

#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct Ended<O> {
    pub identity: Identity,
    pub outcome: O,
}

#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
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

/// The trace of one Nexus's processes.
pub type Traced<P> = Trace<P, <P as Processable>::Gift, <P as Processable>::Taken, <P as Processable>::Outcome>;

/// What the queue yields: a process to be born, or a thing for a living one.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Arrival<P: Processable> {
    Born { owner: Option<Identity>, process: P },
    Given(Given<P::Taken>),
}

/// Where the trace goes: every gift is carried from here, every end announced.
pub trait Tracing<P: Processable> {
    fn trace(&mut self, trace: Traced<P>);
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

/// What a body is handed: the world, bound to the identity of the process.
struct Giver<'a, W, P> {
    identity: &'a Identity,
    world: &'a mut W,
    process: PhantomData<fn() -> P>,
}

impl<P: Processable, W: Tracing<P>> Giving<P::Gift> for Giver<'_, W, P> {
    fn give<N: Givable + Into<P::Gift>>(&mut self, gift: N) {
        self.world.trace(Trace::Gave(Gave { identity: self.identity.clone(), gift: gift.into() }));
    }
}

impl<P> ProcessManager<P>
where
    P: Processable + Clone,
    P::Taken: Clone,
    P::Outcome: Unwindable,
{
    /// The manager's one queue is any iterator of arrivals: its next waits for
    /// the next thing, and its end ends every process.
    pub fn run<Q: IntoIterator<Item = Arrival<P>>, W: Tracing<P>>(&mut self, queue: Q, world: &mut W) {
        for arrival in queue {
            match arrival {
                Arrival::Born { owner, process } => self.bear(owner, process, world),
                Arrival::Given(given) => self.hand(given, world),
            }
        }
        self.end_all(world);
    }

    pub fn bear<W: Tracing<P>>(&mut self, owner: Option<Identity>, process: P, world: &mut W) {
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
        let outcome = living.process.begin(Giver { identity: &living.identity, world: &mut *world, process: PhantomData });
        let identity = living.identity.clone();
        self.table.push(Entry { living, born: 0 });
        if let Some(outcome) = outcome {
            self.end(identity, outcome, world);
        }
    }

    /// Hands a thing to the process it is for; a thing for an ended process is let go.
    pub fn hand<W: Tracing<P>>(&mut self, given: Given<P::Taken>, world: &mut W) {
        let Some(entry) = self.table.iter_mut().find(|entry| entry.living.identity == given.identity) else {
            return;
        };
        world.trace(Trace::Took(given.clone()));
        let living = &mut entry.living;
        let giver = Giver { identity: &living.identity, world: &mut *world, process: PhantomData };
        if let Some(outcome) = living.process.take(given.taken, giver) {
            self.end(given.identity, outcome, world);
        }
    }

    /// A reverse unwind: what the process owns ends first, the last born first.
    pub fn end<W: Tracing<P>>(&mut self, identity: Identity, outcome: P::Outcome, world: &mut W) {
        let owned: Vec<Identity> = self.owned_by(&identity).collect();
        for owned in owned.into_iter().rev() {
            self.end(owned, P::Outcome::unwound(), world);
        }
        self.table.retain(|entry| entry.living.identity != identity);
        world.trace(Trace::Ended(Ended { identity, outcome }));
    }

    /// Every living process, ended the last born first.
    pub fn end_all<W: Tracing<P>>(&mut self, world: &mut W) {
        let roots: Vec<Identity> = self
            .table
            .iter()
            .map(|entry| entry.living.identity.clone())
            .filter(|identity| identity.path.0.is_empty())
            .collect();
        for root in roots.into_iter().rev() {
            self.end(root, P::Outcome::unwound(), world);
        }
    }

    fn owned_by<'a>(&'a self, owner: &'a Identity) -> impl Iterator<Item = Identity> + 'a {
        self.table
            .iter()
            .map(|entry| &entry.living.identity)
            .filter(move |identity| {
                identity.root == owner.root
                    && identity.path.0.len() == owner.path.0.len() + 1
                    && identity.path.0.starts_with(&owner.path.0)
            })
            .cloned()
    }
}
