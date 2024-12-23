use nullable::{NullableState, Source, SqlFlavour, Table};

#[test]
pub fn wildcard_1() {
    let foo_table = Table::new("foo")
        .push_column("id", false)
        .push_column("name", false);

    let source = Source::new(vec![foo_table]);

    let query = r#"
        SELECT * FROM foo
    "#;

    let mut state = NullableState::new(query, source, SqlFlavour::Postgres);
    let nullable = state.get_nullable(&["id", "name"]);
    println!("{:?}", nullable);
    assert!(nullable == [false, false])
}

#[test]
pub fn wildcard_2() {
    let foo_table = Table::new("foo")
        .push_column("foo_id", false)
        .push_column("foo_name", false);

    let bar_table = Table::new("bar")
        .push_column("bar_id", false)
        .push_column("bar_name", false);

    let source = Source::new(vec![foo_table, bar_table]);

    let query = r#"
        SELECT * FROM foo, bar
    "#;

    let mut state = NullableState::new(query, source, SqlFlavour::Postgres);
    let nullable = state.get_nullable(&["foo_id", "foo_name", "bar_id", "bar_name"]);
    println!("{:?}", nullable);
    assert!(nullable == [false, false, false, false])
}

#[test]
pub fn wildcard_3() {
    let user_table = Table::new("users")
        .push_column("id", false)
        .push_column("username", true)
        .push_column("emailadres", true)
        .push_column("pet_id", false);

    let pets_table = Table::new("pets")
        .push_column("pet_id", false)
        .push_column("pet_name", false);

    let source = Source::new(vec![user_table, pets_table]);

    let query = r#"
select
	users.id,
	users.username,
	pets.*
from
	users
left join pets on pets.pet_id = users.pet_id
    "#;

    let mut state = NullableState::new(query, source, SqlFlavour::Postgres);
    let nullable = state.get_nullable(&["id", "username", "pet_id", "pet_name"]);
    println!("found: {:?}", nullable);
    assert!(nullable == [false, true, true, true])
}

#[test]
pub fn wildcard_4() {
    let source = Source::empty();

    let query = r#"
        SELECT * FROM (VALUES (1, 'one'), (2, 'two'), (3, 'three')) AS t (num,letter);
    "#;

    let mut state = NullableState::new(query, source, SqlFlavour::Postgres);
    let nullable = state.get_nullable(&["num", "letter"]);
    println!("found: {:?}", nullable);
    assert!(nullable == [false, false])
}
