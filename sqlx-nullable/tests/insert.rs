use nullable::{NullableState, Source, SqlFlavour, Table};

#[test]
pub fn insert_1() {
    let table_1 = Table::new("pets")
        .push_column("pet_id", false)
        .push_column("pet_name", false);

    let source = Source::new(vec![table_1]);

    let query = r#"
insert into pets(pet_name) values ('pet 1'), ('pet 2'), ('pet 3'), ('pet 4') returning *;
    "#;

    let mut state = NullableState::new(query, source, SqlFlavour::Postgres);
    let nullable = state.get_nullable(&["pet_id", "pet_name"]);
    println!("{:?}", nullable);
    assert!(nullable == [false, false])
}
