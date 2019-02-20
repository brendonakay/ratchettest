package mypkg

func Query(minId int) string {
    return fmt.Sprintf(`SELECT id, name FROM users 
        WHERE id >= %v`, minId)
}
