use nullable::{NullableState, Source, SqlFlavour, Table};

#[test]
pub fn with_1() {
    let source = Source::empty();

    let query = r#"
with user_id as (
    select 1 as id
)
select
	id
from
	user_id
 "#;

    let mut state = NullableState::new(query, source, SqlFlavour::Postgres);
    let nullable = state.get_nullable(&["id"]);
    println!("{:?}", nullable);
    assert!(nullable == [false])
}

#[test]
pub fn with_2() {
    let table_1 = Table::new("pets")
        .push_column("pet_id", false)
        .push_column("pet_name", false);

    let source = Source::new(vec![table_1]);

    let query = r#"
with new_pets as (
	insert into pets(pet_name) values ('pet 1'), ('pet 2'), ('pet 3'), ('pet 4') returning *
)
select * from new_pets
 "#;

    let mut state = NullableState::new(query, source, SqlFlavour::Postgres);
    let nullable = state.get_nullable(&["pet_id", "pet_name"]);
    println!("{:?}", nullable);
    assert!(nullable == [false, false])
}

#[test]
pub fn with_3() {
    let table_1 = Table::new("pets")
        .push_column("pet_id", false)
        .push_column("pet_name", false);

    let source = Source::new(vec![table_1]);

    let query = r#"
with new_pets as (
    update pets set pet_name = '1' where pet_id = 17 returning *)
select * from new_pets
 "#;

    let mut state = NullableState::new(query, source, SqlFlavour::Postgres);
    let nullable = state.get_nullable(&["pet_id", "pet_name"]);
    println!("{:?}", nullable);
    assert!(nullable == [false, false])
}

#[test]
pub fn with_4() {
    let source = Source::empty();

    let query = r#"
WITH table_1 AS (
SELECT GENERATE_SERIES('2012-06-29', '2012-07-03', '1 day'::INTERVAL) AS date
)

, table_2 AS (
SELECT GENERATE_SERIES('2012-06-30', '2012-07-13', '1 day'::INTERVAL) AS date
)

SELECT *
FROM
     table_1 t1
     INNER JOIN
     table_2 t2
     ON t1.date = t2.date
;
 "#;

    let mut state = NullableState::new(query, source, SqlFlavour::Postgres);
    let nullable = state.get_nullable(&["date", "date"]);
    println!("{:?}", nullable);
    assert!(nullable == [false, false])
}


#[test]
pub fn with_5() {
    let source = Source::empty();

    let query = r#"
        with first as (
            select 1
        )
        select * from first

 "#;

    let mut state = NullableState::new(query, source, SqlFlavour::Postgres);
    let nullable = state.get_nullable(&["?column?"]);
    println!("{:?}", nullable);
    assert!(nullable == [false])
}
