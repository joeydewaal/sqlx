use nullable::{NullableState, Source, SqlFlavour, Table};

#[test]
pub fn basic_double_left_inner_join_union() {
    let user_table = Table::new("users")
        .push_column("id", false)
        .push_column("username", false)
        .push_column("emailadres", true)
        .push_column("pet_id", true);

    let pets_table = Table::new("pets")
        .push_column("pet_id", false)
        .push_column("pet_name", false)
        .push_column("plant_id", true);

    let plants_table = Table::new("plants")
        .push_column("plant_id", false)
        .push_column("plant_name", false);

    let source = Source::new(vec![user_table, pets_table, plants_table]);

    let query = r#"
        select
            users.id,
            users.username,
            pets.pet_id,
            pets.pet_name,
            plants.plant_id,
            plants.plant_name
        from
            users
        left join
            pets
        on
            pets.pet_id = users.pet_id
        inner join
            plants
        on
            plants.plant_id = pets.plant_id
        union
        select
            users.id,
            users.username,
            pets.pet_id,
            pets.pet_name,
            plants.plant_id,
            plants.plant_name
        from
            users
        left join
            pets
        on
            pets.pet_id = users.pet_id
        inner join
            plants
        on
            plants.plant_id = pets.plant_id
 "#;

    let mut state = NullableState::new(query, source, SqlFlavour::Postgres);
    let nullable = state.get_nullable(&[
        "id",
        "username",
        "pet_id",
        "pet_name",
        "plant_id",
        "plant_name",
    ]);
    println!("{:?}", nullable);
    assert!(nullable == [false, false, true, true, true, true])
}

#[test]
pub fn basic_double_left_inner_join_double_union() {
    let user_table = Table::new("users")
        .push_column("id", false)
        .push_column("username", false)
        .push_column("emailadres", true)
        .push_column("pet_id", true);

    let pets_table = Table::new("pets")
        .push_column("pet_id", false)
        .push_column("pet_name", false)
        .push_column("plant_id", true);

    let plants_table = Table::new("plants")
        .push_column("plant_id", false)
        .push_column("plant_name", false);

    let source = Source::new(vec![user_table, pets_table, plants_table]);

    let query = r#"
        select
            users.id,
            users.username,
            pets.pet_id,
            pets.pet_name,
            plants.plant_id,
            plants.plant_name
        from
            users
        left join
            pets
        on
            pets.pet_id = users.pet_id
        inner join
            plants
        on
            plants.plant_id = pets.plant_id
        union
        select
            users.id,
            users.username,
            pets.pet_id,
            pets.pet_name,
            plants.plant_id,
            plants.plant_name
        from
            users
        left join
            pets
        on
            pets.pet_id = users.pet_id
        inner join
            plants
        on
            plants.plant_id = pets.plant_id
        union
        select
            users.id,
            users.username,
            pets.pet_id,
            pets.pet_name,
            plants.plant_id,
            plants.plant_name
        from
            users
        left join
            pets
        on
            pets.pet_id = users.pet_id
        inner join
            plants
        on
            plants.plant_id = pets.plant_id
 "#;

    let mut state = NullableState::new(query, source, SqlFlavour::Postgres);
    let nullable = state.get_nullable(&[
        "id",
        "username",
        "pet_id",
        "pet_name",
        "plant_id",
        "plant_name",
    ]);
    println!("{:?}", nullable);
    assert!(nullable == [false, false, true, true, true, true])
}
#[test]
pub fn basic_select1() {
    let user_table = Table::new("users")
        .push_column("id", false)
        .push_column("username", false)
        .push_column("emailadres", true);

    let source = Source::new(vec![user_table]);

    let query = r#"
        select users.id, username, emailadres from users
    "#;

    let mut state = NullableState::new(query, source, SqlFlavour::Postgres);
    let nullable = state.get_nullable(&["id", "username", "emailadres"]);
    println!("{:?}", nullable);
    assert!(nullable == [false, false, true])
}

#[test]
pub fn select_static() {
    let user_table = Table::new("users")
        .push_column("id", false)
        .push_column("username", false)
        .push_column("emailadres", true);

    let source = Source::new(vec![user_table]);

    let query = r#"
     select
         1 as value

 "#;

    let mut state = NullableState::new(query, source, SqlFlavour::Postgres);
    let nullable = state.get_nullable(&["value"]);
    println!("{:?}", nullable);
    assert!(nullable == [false])
}

#[test]
pub fn select_static_multiple() {
    let query = r#"
    select
        1 as value, null as value2
    union
    select
        2 as value, 3 as value2

"#;
    let mut state = NullableState::new(query, Source::empty(), SqlFlavour::Postgres);
    let nullable = state.get_nullable(&["value", "value2"]);
    println!("{:?}", nullable);
    assert!(nullable == [false, true])
}

#[test]
pub fn basic_left_join() {
    let user_table = Table::new("users")
        .push_column("id", false)
        .push_column("username", false)
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
            users.emailadres,
            pets.pet_id,
            pets.pet_name
        from
            users
        left join
            pets
        on
            pets.pet_id = users.pet_id
 "#;

    let mut state = NullableState::new(query, source, SqlFlavour::Postgres);
    let nullable = state.get_nullable(&["id", "username", "emailadres", "pet_id", "pet_name"]);
    println!("{:?}", nullable);
    assert!(nullable == [false, false, true, true, true])
}

#[test]
pub fn basic_inner_join() {
    let user_table = Table::new("users")
        .push_column("id", false)
        .push_column("username", false)
        .push_column("emailadres", true)
        .push_column("pet_id", true);

    let pets_table = Table::new("pets")
        .push_column("pet_id", false)
        .push_column("pet_name", false);

    let source = Source::new(vec![user_table, pets_table]);

    let query = r#"
        select
            users.id,
            users.username,
            users.emailadres,
            pets.pet_id,
            pets.pet_name
        from
            users
        inner join
            pets
        on
            pets.pet_id = users.pet_id
 "#;

    let mut state = NullableState::new(query, source, SqlFlavour::Postgres);
    let nullable = state.get_nullable(&["id", "username", "emailadres", "pet_id", "pet_name"]);
    println!("{:?}", nullable);
    assert!(nullable == [false, false, true, false, false])
}

#[test]
pub fn basic_double_left_join() {
    let user_table = Table::new("users")
        .push_column("id", false)
        .push_column("username", false)
        .push_column("emailadres", true)
        .push_column("pet_id", true)
        .push_column("plant_id", true);

    let pets_table = Table::new("pets")
        .push_column("pet_id", false)
        .push_column("pet_name", false);

    let plants_table = Table::new("plants")
        .push_column("plant_id", false)
        .push_column("plant_name", false);

    let source = Source::new(vec![user_table, pets_table, plants_table]);

    let query = r#"
        select
            users.id,
            users.username,
            pets.pet_id,
            pets.pet_name,
            plants.plant_id,
            plants.plant_name
        from
            users
        left join
            pets
        on
            pets.pet_id = users.pet_id
        left join
            plants
        on
            plants.plant_id = users.plant_id
 "#;

    let mut state = NullableState::new(query, source, SqlFlavour::Postgres);
    let nullable = state.get_nullable(&[
        "id",
        "username",
        "pet_id",
        "pet_name",
        "plant_id",
        "plant_name",
    ]);
    println!("{:?}", nullable);
    assert!(nullable == [false, false, true, true, true, true])
}

#[test]
pub fn basic_double_left_inner_join() {
    let user_table = Table::new("users")
        .push_column("id", false)
        .push_column("username", false)
        .push_column("emailadres", true)
        .push_column("pet_id", true);

    let pets_table = Table::new("pets")
        .push_column("pet_id", false)
        .push_column("pet_name", false)
        .push_column("plant_id", true);

    let plants_table = Table::new("plants")
        .push_column("plant_id", false)
        .push_column("plant_name", false);

    let source = Source::new(vec![user_table, pets_table, plants_table]);

    let query = r#"
        select
            users.id,
            users.username,
            pets.pet_id,
            pets.pet_name,
            plants.plant_id,
            plants.plant_name
        from
            users
        left join
            pets
        on
            pets.pet_id = users.pet_id
        inner join
            plants
        on
            plants.plant_id = pets.plant_id
 "#;

    let mut state = NullableState::new(query, source, SqlFlavour::Postgres);
    let nullable = state.get_nullable(&[
        "id",
        "username",
        "pet_id",
        "pet_name",
        "plant_id",
        "plant_name",
    ]);
    println!("{:?}", nullable);
    assert!(nullable == [false, false, true, true, true, true])
}

#[test]
pub fn select_count1() {
    let user_table = Table::new("users")
        .push_column("id", false)
        .push_column("username", false)
        .push_column("emailadres", true);

    let source = Source::new(vec![user_table]);

    let query = r#"
        select
            count(users.id)
        from
            users
 "#;

    let mut state = NullableState::new(query, source, SqlFlavour::Postgres);
    let nullable = state.get_nullable(&["?column?"]);
    println!("{:?}", nullable);
    assert!(nullable == [false])
}

#[test]
pub fn select_hardcoded_value() {
    let source = Source::empty();

    let query = r#"
        SELECT 1
 "#;

    let mut state = NullableState::new(query, source, SqlFlavour::Postgres);
    let nullable = state.get_nullable(&["?column?"]);
    println!("{:?}", nullable);
    assert!(nullable == [false])
}

#[test]
pub fn select_cast1() {
    let source = Source::empty();

    let query = r#"
        SELECT '123'::INTEGER;

 "#;

    let mut state = NullableState::new(query, source, SqlFlavour::Postgres);
    let nullable = state.get_nullable(&["?column?"]);
    println!("{:?}", nullable);
    assert!(nullable == [false])
}

#[test]
pub fn select_cast2() {
    let source = Source::empty();

    let query = r#"
        SELECT NULL::INTEGER;

 "#;

    let mut state = NullableState::new(query, source, SqlFlavour::Postgres);
    let nullable = state.get_nullable(&["?column?"]);
    println!("{:?}", nullable);
    assert!(nullable == [true])
}

#[test]
pub fn select_func() {
    let source = Source::empty();

    let query = r#"
        SELECT now();
 "#;

    let mut state = NullableState::new(query, source, SqlFlavour::Postgres);
    let nullable = state.get_nullable(&["?column?"]);
    println!("{:?}", nullable);
    assert!(nullable == [false])
}

// #[test]
// pub fn basic_double_left_inner_join_union_mixed() {
//     let user_table = Table::new("users")
//         .push_column("id", false)
//         .push_column("username", false)
//         .push_column("pet_id", true);

//     let pets_table = Table::new("pets")
//         .push_column("pet_id", false)
//         .push_column("pet_name", false)
//         .push_column("plant_id", true);

//     let plants_table = Table::new("plants")
//         .push_column("plant_id", false)
//         .push_column("plant_name", false);

//     let source = Source::new(vec![user_table, pets_table, plants_table]);

//     let query = r#"
//         select
//             users.id,
//             users.username,
//             pets.pet_id,
//             pets.pet_name
//         from
//             users
//         left join
//             pets
//         on
//             pets.pet_id = users.pet_id
//         union
//         select
//             pets.pet_name,
//             users.username,
//             pets.pet_id,
//             users.id
//         from
//             users
//         inner join
//             pets
//         on
//             pets.pet_id = users.pet_id
//  "#;

//     let mut state = NullableState::new(query, source, SqlFlavour::Postgres);
//     let nullable = state.get_nullable(&[
//         "id",
//         "username",
//         "pet_id",
//         "pet_name"
//     ]);
//     println!("{:?}", nullable);
//     assert!(nullable == [false, false, true, true])
// }

#[test]
pub fn select_exists1() {
    let source = Source::empty();

    let query = r#"
        SELECT EXISTS (SELECT 1);
 "#;

    let mut state = NullableState::new(query, source, SqlFlavour::Postgres);
    let nullable = state.get_nullable(&["?colun?"]);
    println!("{:?}", nullable);
    assert!(nullable == [false])
}

#[test]
pub fn select_not_exists1() {
    let source = Source::empty();

    let query = r#"
        SELECT NOT EXISTS (SELECT 1);
 "#;

    let mut state = NullableState::new(query, source, SqlFlavour::Postgres);
    let nullable = state.get_nullable(&["?colun?"]);
    println!("{:?}", nullable);
    assert!(nullable == [false])
}

#[test]
pub fn select_func1() {
    let user_table = Table::new("users")
        .push_column("id", false)
        .push_column("username", false)
        .push_column("age", false);

    let source = Source::new(vec![user_table]);

    let query = r#"
        select
            avg(users.age)
        from
            users
 "#;

    let mut state = NullableState::new(query, source, SqlFlavour::Postgres);
    let nullable = state.get_nullable(&["?colun?"]);
    println!("{:?}", nullable);
    assert!(nullable == [false])
}

#[test]
pub fn select_func2() {
    let user_table = Table::new("users")
        .push_column("id", false)
        .push_column("username", false)
        .push_column("age", true);

    let source = Source::new(vec![user_table]);

    let query = r#"
        select
            avg(users.age), upper(username)
        from
            users
 "#;

    let mut state = NullableState::new(query, source, SqlFlavour::Postgres);
    let nullable = state.get_nullable(&["?colun?", "?colun?"]);
    println!("{:?}", nullable);
    assert!(nullable == [true, false])
}

#[test]
pub fn select_func3() {
    let source = Source::empty();

    let query = r#"
        select
            coalesce(null, 1),
            coalesce(null),
            coalesce()
 "#;

    let mut state = NullableState::new(query, source, SqlFlavour::Postgres);
    let nullable = state.get_nullable(&["coalesce", "coalesce", "coalesce"]);
    println!("{:?}", nullable);
    assert!(nullable == [false, true, true])
}

#[test]
pub fn basic_left_join_func1() {
    let user_table = Table::new("users")
        .push_column("id", false)
        .push_column("username", false)
        .push_column("emailadres", true)
        .push_column("pet_id", false);

    let pets_table = Table::new("pets")
        .push_column("pet_id", false)
        .push_column("pet_name", false)
        .push_column("age", false);

    let source = Source::new(vec![user_table, pets_table]);

    let query = r#"
        select
            users.id,
            users.username,
            users.emailadres,
            pets.pet_id,
            pets.pet_name,
            avg(pets.age)
        from
            users
        inner join
            pets
        on
            pets.pet_id = users.pet_id
 "#;

    let mut state = NullableState::new(query, source, SqlFlavour::Postgres);
    let nullable = state.get_nullable(&[
        "id",
        "username",
        "emailadres",
        "pet_id",
        "pet_name",
        "?column?",
    ]);
    println!("{:?}", nullable);
    assert!(nullable == [false, false, true, false, false, false])
}

#[test]
pub fn basic_right_join_func1() {
    let user_table = Table::new("users")
        .push_column("id", false)
        .push_column("username", false)
        .push_column("emailadres", true)
        .push_column("pet_id", false);

    let pets_table = Table::new("pets")
        .push_column("pet_id", false)
        .push_column("pet_name", false)
        .push_column("age", false);

    let source = Source::new(vec![user_table, pets_table]);

    let query = r#"
        select
            users.id,
            users.username,
            users.emailadres,
            pets.pet_id,
            pets.pet_name,
            avg(pets.age)
        from
            users
        right join
            pets
        on
            pets.pet_id = users.pet_id
 "#;

    let mut state = NullableState::new(query, source, SqlFlavour::Postgres);
    let nullable = state.get_nullable(&[
        "id",
        "username",
        "emailadres",
        "pet_id",
        "pet_name",
        "?column?",
    ]);
    println!("{:?}", nullable);
    assert!(nullable == [true, true, true, false, false, false])
}

#[test]
pub fn double_right_join() {
    let user_table = Table::new("users")
        .push_column("id", false)
        .push_column("name", false)
        .push_column("pet_id", true)
        .push_column("company_id", true);

    let pets_table = Table::new("pets")
        .push_column("pet_id", false)
        .push_column("pet_name", false);

    let company_table = Table::new("company")
        .push_column("id", false)
        .push_column("name", false);

    let source = Source::new(vec![user_table, pets_table, company_table]);

    let query = r#"
        select
            users.id,
            users.name,
            company.id,
            company.name,
            pets.pet_id,
            pets.pet_name
        from
            users
        inner join
            pets
        on
            pets.pet_id = users.pet_id
        right join
            company
        on
            company.id = users.company_id
     "#;

    let mut state = NullableState::new(query, source, SqlFlavour::Postgres);
    let nullable = state.get_nullable(&["id", "name", "id", "name", "pet_id", "pet_name"]);
    println!("{:?}", nullable);
    assert!(nullable == [true, true, false, false, true, true])
}

#[test]
pub fn double_right_join2() {
    let user_table = Table::new("users")
        .push_column("id", false)
        .push_column("name", false)
        .push_column("pet_id", true)
        .push_column("company_id", true);

    let pets_table = Table::new("pets")
        .push_column("pet_id", false)
        .push_column("pet_name", false);

    let company_table = Table::new("company")
        .push_column("id", false)
        .push_column("name", false);

    let source = Source::new(vec![user_table, pets_table, company_table]);

    let query = r#"
        select
            users.id,
            users.name,
            company.id,
            company.name,
            pets.pet_id,
            pets.pet_name
        from
            users
        right join
            company
        on
            company.id = users.company_id
        inner join
            pets
        on
            pets.pet_id = users.pet_id
 "#;

    let mut state = NullableState::new(query, source, SqlFlavour::Postgres);
    let nullable = state.get_nullable(&["id", "name", "id", "name", "pet_id", "pet_name"]);
    println!("{:?}", nullable);
    assert!(nullable == [true, true, false, false, true, true])
}

#[test]
pub fn double_right_join_3() {
    let user_table = Table::new("users")
        .push_column("user_id", false)
        .push_column("name", false)
        .push_column("emailadres", true)
        .push_column("age", true)
        .push_column("pet_id", true);

    let pets_table = Table::new("pets")
        .push_column("pet_id", false)
        .push_column("pet_name", false)
        .push_column("plant_id", false);

    let plants_table = Table::new("plants")
        .push_column("plant_id", false)
        .push_column("plant_name", false);

    let source = Source::new(vec![user_table, pets_table, plants_table]);

    let query = r#"
        select
            u.user_id,
            u.name,
            p.pet_id,
            p.pet_name,
            pl.plant_id,
            pl.plant_name
        from
            users as u
        left join
            pets as p
        on
            p.pet_id = u.pet_id
        right join
            plants as pl
        on
            pl.plant_id = p.plant_id
     "#;

    let mut state = NullableState::new(query, source, SqlFlavour::Postgres);
    let nullable = state.get_nullable(&["user_id", "name", "pet_id", "pet_name", "plant_id", "plant_name"]);
    println!("{:?}", nullable);
    assert!(nullable == [true, true, true, true, false, false])
}
