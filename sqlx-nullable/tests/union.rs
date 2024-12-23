use nullable::{NullableState, Source, SqlFlavour};

#[test]
pub fn union1() {
    let source = Source::empty();

    let query = r#"
with user_id as (
	select 1 as test, 2 as test1
)
select
	user_id.test, test1
from
	user_id
union
select 1, 2 as test3
 "#;

    let mut state = NullableState::new(query, source, SqlFlavour::Postgres);
    let nullable = state.get_nullable(&["test", "test3"]);
    println!("{:?}", nullable);
    assert!(nullable == [false, false])
}

#[test]
pub fn union2() {
    let source = Source::empty();

    let query = r#"
with user_id as (
	select 1 as test, 2 as test1
)
select 1, 2 as test3
union
select
	user_id.test, test1
from
	user_id
 "#;

    let mut state = NullableState::new(query, source, SqlFlavour::Postgres);
    let nullable = state.get_nullable(&["test", "test3"]);
    println!("{:?}", nullable);
    assert!(nullable == [false, false])
}

#[test]
pub fn union3() {
    let source = Source::empty();

    let query = r#"
with user_id as (
	select 1 as test, 2 as test1
)
select 1, 2 as test3 where 1 < (select test from user_id)
union
select
	user_id.test, test1
from
	user_id

 "#;

    let mut state = NullableState::new(query, source, SqlFlavour::Postgres);
    let nullable = state.get_nullable(&["test", "test3"]);
    println!("{:?}", nullable);
    assert!(nullable == [false, false])
}
