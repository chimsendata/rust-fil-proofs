use std::sync::{Mutex, MutexGuard};

use anyhow::{format_err, Result};
use hwloc::{Bitmap, ObjectType, Topology, TopologyObject, CPUBIND_THREAD};
use lazy_static::lazy_static;
use log::{debug, info, warn};
use serde_json::from_str;
use storage_proofs_core::settings::SETTINGS;

type CoreGroup = Vec<CoreIndex>;
lazy_static! {
    pub static ref TOPOLOGY: Mutex<Topology> = Mutex::new(Topology::new());
    pub static ref CORE_GROUPS: Option<Vec<Mutex<CoreGroup>>> = {
        // let num_producers = &SETTINGS.multicore_sdr_producers;
        // let cores_per_unit = num_producers + 1;
        let core_plan = SETTINGS.multicore_sdr_core_plan.clone();

        core_groups(core_plan)
    };
}

#[derive(Clone, Copy, Debug, PartialEq)]
/// `CoreIndex` is a simple wrapper type for indexes into the set of vixible cores. A `CoreIndex` should only ever be
/// created with a value known to be less than the number of visible cores.
pub struct CoreIndex(usize);

pub fn checkout_core_group() -> Option<MutexGuard<'static, CoreGroup>> {
    match &*CORE_GROUPS {
        Some(groups) => {
            for (i, group) in groups.iter().enumerate() {
                match group.try_lock() {
                    Ok(guard) => {
                        debug!("checked out core group {}", i);
                        return Some(guard);
                    }
                    Err(_) => debug!("core group {} locked, could not checkout", i),
                }
            }
            None
        }
        None => None,
    }
}

#[cfg(not(target_os = "windows"))]
pub type ThreadId = libc::pthread_t;

#[cfg(target_os = "windows")]
pub type ThreadId = winapi::winnt::HANDLE;

/// Helper method to get the thread id through libc, with current rust stable (1.5.0) its not
/// possible otherwise I think.
#[cfg(not(target_os = "windows"))]
fn get_thread_id() -> ThreadId {
    unsafe { libc::pthread_self() }
}

#[cfg(target_os = "windows")]
fn get_thread_id() -> ThreadId {
    unsafe { kernel32::GetCurrentThread() }
}

pub struct Cleanup {
    tid: ThreadId,
    prior_state: Option<Bitmap>,
}

impl Drop for Cleanup {
    fn drop(&mut self) {
        if let Some(prior) = self.prior_state.take() {
            let child_topo = &TOPOLOGY;
            let mut locked_topo = child_topo.lock().expect("poisded lock");
            let _ = locked_topo.set_cpubind_for_thread(self.tid, prior.clone(), CPUBIND_THREAD);
            let _ = locked_topo.set_membind(prior, hwloc::MEMBIND_DEFAULT, hwloc::MEMBIND_THREAD);
        }
    }
}

pub fn bind_core(core_index: CoreIndex) -> Result<Cleanup> {
    let child_topo = &TOPOLOGY;
    let tid = get_thread_id();
    let mut locked_topo = child_topo.lock().expect("poisoned lock");
    let core = get_core_by_index(&locked_topo, core_index)
        .map_err(|err| format_err!("failed to get core at index {}: {:?}", core_index.0, err))?;

    let cpuset = core
        .allowed_cpuset()
        .ok_or_else(|| format_err!("no allowed cpuset for core at index {}", core_index.0,))?;
    debug!("allowed cpuset: {:?}", cpuset);
    let mut bind_to = cpuset;

    // Get only one logical processor (in case the core is SMT/hyper-threaded).
    bind_to.singlify();

    // Thread binding before explicit set.
    let before = locked_topo.get_cpubind_for_thread(tid, CPUBIND_THREAD);

    debug!("binding to {:?}", bind_to);
    // Set the binding.
    let result = locked_topo
        // .set_cpubind_for_thread(tid, bind_to, CPUBIND_THREAD)
        .set_cpubind_for_thread(tid, bind_to.clone(), CPUBIND_THREAD)
        .map_err(|err| format_err!("failed to bind CPU: {:?}", err));

    if result.is_err() {
        warn!("error in bind_core, {:?}", result);
    }

    let _ = locked_topo.set_membind(bind_to, hwloc::MEMBIND_BIND, hwloc::MEMBIND_THREAD);

    Ok(Cleanup {
        tid,
        prior_state: before,
    })
}

fn get_core_by_index(topo: &Topology, index: CoreIndex) -> Result<&TopologyObject> {
    let idx = index.0;

    match topo.objects_with_type(&ObjectType::Core) {
        Ok(all_cores) if idx < all_cores.len() => Ok(all_cores[idx]),
        Ok(all_cores) => Err(format_err!(
            "idx ({}) out of range for {} cores",
            idx,
            all_cores.len()
        )),
        _e => Err(format_err!("failed to get core by index {}", idx,)),
    }
}

fn core_groups(core_plan: String) -> Option<Vec<Mutex<Vec<CoreIndex>>>> {
    // let topo = TOPOLOGY.lock().expect("poisoned lock");
    //
    // let a = topo.objects_with_type(&ObjectType::NUMANode)
    //     .expect("objects_with_type failed");
    //
    // for b in a {
    //     println!("{:?}", b.cpuset().unwrap());
    //     // println!("{:?}",b.cpuset())
    // }
    //
    // let core_depth = match topo.depth_or_below_for_type(&ObjectType::Core) {
    //     Ok(depth) => depth,
    //     Err(_) => return None,
    // };
    // let all_cores = topo
    //     .objects_with_type(&ObjectType::Core)
    //     .expect("objects_with_type failed");
    // let core_count = all_cores.len();
    //
    // let mut cache_depth = core_depth;
    // let mut cache_count = 1;
    //
    // while cache_depth > 0 {
    //     let objs = topo.objects_at_depth(cache_depth);
    //     let obj_count = objs.len();
    //     if obj_count < core_count {
    //         cache_count = obj_count;
    //         break;
    //     }
    //
    //     cache_depth -= 1;
    // }
    //
    // assert_eq!(0, core_count % cache_count);
    // let mut group_size = core_count / cache_count;
    // let mut group_count = cache_count;
    //
    // if cache_count <= 1 {
    //     // If there are not more than one shared caches, there is no benefit in trying to group cores by cache.
    //     // In that case, prefer more groups so we can still bind cores and also get some parallelism.
    //     // Create as many full groups as possible. The last group may not be full.
    //     group_count = core_count / cores_per_unit;
    //     group_size = cores_per_unit;
    //
    //     info!(
    //         "found only {} shared cache(s), heuristically grouping cores into {} groups",
    //         cache_count, group_count
    //     );
    // } else {
    //     debug!(
    //         "Cores: {}, Shared Caches: {}, cores per cache (group_size): {}",
    //         core_count, cache_count, group_size
    //     );
    // }
    //
    // let skips: Vec<CoreIndex>;
    // if skip_cores.eq("") {
    //     skips = Vec::new();
    // } else {
    //     skips = skip_cores.split(",")
    //         .into_iter()
    //         .map(|core|CoreIndex(from_str::<usize>(core).unwrap()))
    //         .collect::<Vec<_>>();
    //     println!("{:?}",skips);
    // }
    //
    // let core_groups = (0..group_count)
    //     .map(|i| {
    //         (0..group_size)
    //             .map(|j| {
    //                 let core_index = i * group_size + j;
    //                 assert!(core_index < core_count);
    //                 CoreIndex(core_index)
    //             })
    //             .collect::<Vec<_>>()
    //     })
    //     .collect::<Vec<_>>();

    let custom_groups = match core_plan.as_str() {
        "DELL7525" => {
            vec![
                // 实核
                vec![CoreIndex(0),CoreIndex(1),CoreIndex(2),CoreIndex(3)],
                vec![CoreIndex(4),CoreIndex(5),CoreIndex(6),CoreIndex(7)],
                vec![CoreIndex(8),CoreIndex(9),CoreIndex(10),CoreIndex(11)],
                vec![CoreIndex(12),CoreIndex(13),CoreIndex(14),CoreIndex(15)],
                // GPU0 vec![CoreIndex(16),CoreIndex(17),CoreIndex(18),CoreIndex(19)],
                vec![CoreIndex(20),CoreIndex(21),CoreIndex(22),CoreIndex(23)],
                vec![CoreIndex(24),CoreIndex(25),CoreIndex(26),CoreIndex(27)],
                vec![CoreIndex(28),CoreIndex(29),CoreIndex(30),CoreIndex(31)],
                // GPU1 vec![CoreIndex(32),CoreIndex(33),CoreIndex(34),CoreIndex(35)],
                vec![CoreIndex(36),CoreIndex(37),CoreIndex(38),CoreIndex(39)],
                vec![CoreIndex(40),CoreIndex(41),CoreIndex(42),CoreIndex(43)],
                vec![CoreIndex(44),CoreIndex(45),CoreIndex(46),CoreIndex(47)],
                vec![CoreIndex(48),CoreIndex(49),CoreIndex(50),CoreIndex(51)],
                vec![CoreIndex(52),CoreIndex(53),CoreIndex(54),CoreIndex(55)],
                vec![CoreIndex(56),CoreIndex(57),CoreIndex(58),CoreIndex(59)],
                vec![CoreIndex(60),CoreIndex(61),CoreIndex(62),CoreIndex(63)],
                // 虚核
                vec![CoreIndex(64),CoreIndex(65),CoreIndex(66),CoreIndex(67)],
                vec![CoreIndex(68),CoreIndex(69),CoreIndex(70),CoreIndex(71)],
                vec![CoreIndex(72),CoreIndex(73),CoreIndex(74),CoreIndex(75)],
                vec![CoreIndex(76),CoreIndex(77),CoreIndex(78),CoreIndex(79)],
                // GPU0 vec![CoreIndex(80),CoreIndex(81),CoreIndex(82),CoreIndex(83)],
                vec![CoreIndex(84),CoreIndex(85),CoreIndex(86),CoreIndex(87)],
                vec![CoreIndex(88),CoreIndex(89),CoreIndex(90),CoreIndex(91)],
                vec![CoreIndex(92),CoreIndex(93),CoreIndex(94),CoreIndex(95)],
                // GPU1 vec![CoreIndex(96),CoreIndex(97),CoreIndex(98),CoreIndex(99)],
                vec![CoreIndex(100),CoreIndex(101),CoreIndex(102),CoreIndex(103)],
                vec![CoreIndex(104),CoreIndex(105),CoreIndex(106),CoreIndex(107)],
                vec![CoreIndex(108),CoreIndex(109),CoreIndex(110),CoreIndex(111)],
                vec![CoreIndex(112),CoreIndex(113),CoreIndex(114),CoreIndex(115)],
                vec![CoreIndex(116),CoreIndex(117),CoreIndex(118),CoreIndex(119)],
                vec![CoreIndex(120),CoreIndex(121),CoreIndex(122),CoreIndex(123)],
                vec![CoreIndex(124),CoreIndex(125),CoreIndex(126),CoreIndex(127)],
            ]
        },
        _=> vec![vec![CoreIndex(0),CoreIndex(1),CoreIndex(2),CoreIndex(3)]]
    };

    Some(
        custom_groups
            .iter()
            // .filter(|group| !skips.contains(group.split_first().unwrap().0))
            .map(|group| Mutex::new(group.clone()))
            .collect::<Vec<_>>(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cores() {
        println!("test_cores");
        let cores = core_groups(String::from("PLAN_X"));
        println!("{:?}", cores);
    }

    #[test]
    #[cfg(feature = "isolated-testing")]
    // This test should not be run while other tests are running, as
    // the cores we're working with may otherwise be busy and cause a
    // failure.
    fn test_checkout_cores() {
        let checkout1 = checkout_core_group();
        dbg!(&checkout1);
        let checkout2 = checkout_core_group();
        dbg!(&checkout2);

        // This test might fail if run on a machine with fewer than four cores.
        match (checkout1, checkout2) {
            (Some(c1), Some(c2)) => assert!(*c1 != *c2),
            _ => panic!("failed to get two checkouts"),
        }
    }
}
