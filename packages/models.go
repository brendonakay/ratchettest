package mypkg
// I like to move these structs to a file called models.go
type ReceivedData struct {
	ID int `json:"id,omitempty"`
}
type TransformedData struct {
	UserID       int    `json:"user_id,omitempty"`
	SomeNewField string `json:"some_new_field"`
}
