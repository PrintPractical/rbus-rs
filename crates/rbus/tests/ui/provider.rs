use rbus::provider::{Context, Fault};

struct NoPath;

#[rbus::provider]
impl NoPath {
    #[rbus(get)]
    fn value(&self, _cx: &Context) -> Result<u32, Fault> {
        Ok(1)
    }
}

struct TwoRoles;

#[rbus::provider(path = "Device.A.")]
impl TwoRoles {
    #[rbus(get, set)]
    fn value(&self, _cx: &Context) -> Result<u32, Fault> {
        Ok(1)
    }
}

struct BadPoll;

#[rbus::provider(path = "Device.B.")]
impl BadPoll {
    #[rbus(get, poll = "2 sec")]
    fn value(&self, _cx: &Context) -> Result<u32, Fault> {
        Ok(1)
    }
}

struct PollSetter;

#[rbus::provider(path = "Device.C.")]
impl PollSetter {
    #[rbus(set, poll = "2s")]
    fn set_value(&self, _cx: &Context, _v: u32) -> Result<(), Fault> {
        Ok(())
    }
}

struct NoValue;

#[rbus::provider(path = "Device.D.")]
impl NoValue {
    #[rbus(set)]
    fn set_value(&self) -> Result<(), Fault> {
        Ok(())
    }
}

struct Nothing;

#[rbus::provider(path = "Device.E.")]
impl Nothing {
    fn helper(&self) {}
}

struct Duplicate;

#[rbus::provider(path = "Device.F.")]
impl Duplicate {
    #[rbus(get)]
    fn value(&self, _cx: &Context) -> Result<u32, Fault> {
        Ok(1)
    }
    #[rbus(get = "Value")]
    fn other(&self, _cx: &Context) -> Result<u32, Fault> {
        Ok(2)
    }
}

struct BadMethodName;

#[rbus::provider(path = "Device.G.")]
impl BadMethodName {
    #[rbus(method = "Do.It()")]
    fn run(&self, _cx: &Context) -> Result<(), Fault> {
        Ok(())
    }
}

fn main() {}
