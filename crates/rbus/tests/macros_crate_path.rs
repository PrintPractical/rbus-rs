//! The macros' `crate` option: generated code names `rbus` by the path
//! given, for crates that rename the dependency. Runs anywhere.

/// `rbus` under another path.
mod renamed {
    pub use ::rbus::*;
}

use renamed::provider::{Context, Fault, Prop, Provider, Table};

#[derive(renamed::Row, Clone, PartialEq, Default)]
#[rbus(crate = "crate::renamed")]
struct Port {
    #[rbus(writable)]
    speed: u32,
}

#[derive(renamed::Model, Default)]
#[rbus(path = "Device.Renamed.", crate = "crate::renamed")]
struct Switch {
    #[rbus(writable)]
    enable: Prop<bool>,
    ports: Table<Port>,
}

struct Fan;

#[renamed::provider(path = "Device.Fan.", crate = "crate::renamed")]
impl Fan {
    #[rbus(get)]
    fn rpm(&self, _cx: &Context) -> Result<u32, Fault> {
        Ok(1200)
    }
}

#[tokio::test]
async fn the_crate_option_is_honoured() {
    let names: Vec<String> = Switch::default()
        .schema()
        .elements()
        .iter()
        .map(|e| e.name().to_owned())
        .collect();
    assert!(
        names.contains(&"Device.Renamed.Enable".to_owned()),
        "{names:?}"
    );
    assert!(
        names.contains(&"Device.Renamed.Ports.{i}.Speed".to_owned()),
        "{names:?}"
    );
    let cx = Context::new(None, "rbus.test.INBOX.1");
    let rpm = Fan.get(&cx, &renamed::name!("Device.Fan.Rpm")).await;
    assert_eq!(
        rpm.map_err(|f: Fault| f.status()),
        Ok(renamed::Value::U32(1200))
    );
}
