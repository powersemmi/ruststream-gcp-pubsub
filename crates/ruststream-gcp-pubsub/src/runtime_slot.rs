//! Where a delivery finds the runtime its broker connected on without holding a count on it.
//!
//! A delayed rejection has to run on the runtime `connect` ran on, and the delivery is the only
//! thing `nack_after` has. A reference-counted handle on every delivery would cost an atomic
//! increment and decrement per message for the sake of a rare call, so the connection registers
//! its runtime here once and every delivery carries a [`RuntimeSlot`]: two integers, copied. The
//! lookup, a lock and a handle clone, is paid on `nack_after` alone.
//!
//! The registry is process-wide because nothing else is reachable from a delivery without a
//! count: the delivery holds the client's handler and plain data, nothing of this crate's that
//! is shared.

use std::sync::{Mutex, MutexGuard, PoisonError};

use tokio::runtime::Handle;

/// The registered runtimes, by slot. A slot freed by a connection that went away is reused with
/// a new generation, so a delivery that outlived its connection finds nothing rather than a
/// stranger's runtime.
static REGISTRY: Mutex<Registry> = Mutex::new(Registry {
    slots: Vec::new(),
    free: Vec::new(),
});

struct Registry {
    slots: Vec<Slot>,
    free: Vec<u32>,
}

struct Slot {
    generation: u32,
    runtime: Option<Handle>,
}

fn registry() -> MutexGuard<'static, Registry> {
    REGISTRY.lock().unwrap_or_else(PoisonError::into_inner)
}

/// The address of a registered runtime, carried by every delivery of the connection that
/// registered it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RuntimeSlot {
    index: u32,
    generation: u32,
}

impl RuntimeSlot {
    /// The runtime the connection behind this slot connected on, or `None` once that connection
    /// is gone.
    pub(crate) fn runtime(self) -> Option<Handle> {
        registry()
            .slots
            .get(self.index as usize)
            .filter(|slot| slot.generation == self.generation)
            .and_then(|slot| slot.runtime.clone())
    }
}

/// A connection's registration: holds the slot for as long as the connection lives and frees it
/// when the connection goes.
#[derive(Debug)]
pub(crate) struct RuntimeRegistration {
    slot: RuntimeSlot,
}

impl RuntimeRegistration {
    /// Registers `runtime` under a fresh slot.
    pub(crate) fn new(runtime: Handle) -> Self {
        let mut registry = registry();
        let slot = if let Some(index) = registry.free.pop() {
            let slot = &mut registry.slots[index as usize];
            slot.runtime = Some(runtime);
            RuntimeSlot {
                index,
                generation: slot.generation,
            }
        } else {
            // Why a panic is acceptable: four billion live connections in one process is not an
            // operational state.
            let index = u32::try_from(registry.slots.len()).expect("fewer than 2^32 connections");
            registry.slots.push(Slot {
                generation: 0,
                runtime: Some(runtime),
            });
            RuntimeSlot {
                index,
                generation: 0,
            }
        };
        drop(registry);
        Self { slot }
    }

    /// The slot every delivery of this connection carries.
    pub(crate) const fn slot(&self) -> RuntimeSlot {
        self.slot
    }
}

impl Drop for RuntimeRegistration {
    fn drop(&mut self) {
        let mut registry = registry();
        if let Some(slot) = registry.slots.get_mut(self.slot.index as usize) {
            slot.runtime = None;
            slot.generation = slot.generation.wrapping_add(1);
            registry.free.push(self.slot.index);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn a_slot_finds_its_runtime_until_the_connection_goes() {
        let registration = RuntimeRegistration::new(Handle::current());
        let slot = registration.slot();
        assert!(
            slot.runtime().is_some(),
            "a live connection's slot resolves"
        );
        drop(registration);
        assert!(slot.runtime().is_none(), "a freed slot resolves to nothing");

        let reused = RuntimeRegistration::new(Handle::current());
        assert!(
            slot.runtime().is_none(),
            "a reused slot must not answer a delivery of the connection that left it"
        );
        assert!(reused.slot().runtime().is_some());
    }
}
