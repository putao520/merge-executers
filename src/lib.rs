use std::fmt::{Debug, Display};
use std::future::Future;
use dashmap::DashMap;
use tokio::sync::broadcast;
use anyhow::Result;

pub struct MergeExecutes<R: Clone + Debug + Send + Sync + 'static + Display> {
    task_group: DashMap<String, broadcast::Sender<R>>,
}

impl<R> MergeExecutes<R>
    where
        R: Clone + Debug + Send + Sync + 'static + Display,
{
    pub fn new() -> Self {
        MergeExecutes {
            task_group: DashMap::new(),
        }
    }
    pub async fn run_task(&self, name: &str, handle: impl Future<Output = Result<R>>) -> Result<R> {
        if self.task_group.contains_key(name) {
            match self.task_group.get(name) {
                None => Err(anyhow::anyhow!("Failed to get task group")),
                Some(item) => {
                    let tx = item.value();
                    let mut rx = tx.subscribe();
                    // println!("new waiter {}", name);
                    let r = rx.recv().await?;
                    // println!("Waiter {} finished -> {}", name, r);
                    Ok(r)
                }
            }
        } else {
            let (tx, _) = broadcast::channel::<R>(3);
            self.task_group.insert(name.to_string(), tx.clone());
            let r = handle.await?;
            // println!("Task {} finished -> {}", name, r);
            if tx.receiver_count() > 0 {
                tx.send(r.clone())?;
            }
            while tx.receiver_count() > 0 {
                tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
            }
            self.task_group.remove(name);
            Ok(r)
        }
    }
}