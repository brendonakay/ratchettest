package mypkg
// I like to move these structs to a file called models.go
type ReceivedData struct {
	//ID int `json:"id,omitempty"`
	ID int `json:"id"`
}
type TransformedData struct {
	UserID       int    `json:"user_id"`
	SomeNewField string `json:"some_new_field"`
}
