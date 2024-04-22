package core

import (
	"log"
	"my-cstack-db/src/backend/api/row"
	"reflect"
	"strconv"
	"strings"
)

type StatementType int
type PrepareStatementStatus int

type InputBuffer struct {
	Args []string
}

const (
	PrepareStatementSuccess = iota
	PrepareStatementFailed
)

const (
	StatementTypeInsert StatementType = iota
	StatementTypeSelect
	StatementTypeInvalid
)

type Statement struct {
	typ  StatementType
	op   string // operation. Example: insert, select
	args []string
	row  row.Row
}

func PrepareStm(ib *InputBuffer, stm *Statement) PrepareStatementStatus {
	stm.op = ib.Args[0]
	stm.args = ib.Args
	switch strings.ToLower(stm.op) {
	case "insert":
		{
			// TODO Creating table struct with code automatically
			// TODO Support generic rows
			stm.typ = StatementTypeInsert

			row := row.UserRow{}
			val := reflect.ValueOf(&row).Elem()

			if len(stm.args) != val.NumField() {
				log.Printf("Prepare statement: Incorrect argument amount, expected %d, found %d\n", len(stm.args), val.NumField())
				return PrepareStatementFailed
			}
			// fill args into row fields
			for i := 0; i < val.NumField(); i++ {
				field := val.Field(i)
				arg := stm.args[i]

				if field.Kind() != reflect.Struct {
					if field.CanSet() {
						switch field.Kind() {
						case reflect.String:
							{
								field.SetString(arg)
							}
						case reflect.Uint64:
							{
								num, err := strconv.ParseUint(arg, 10, 64)
								if err != nil {
									log.Println("Prepare statement: Failed to convert arg to uint64")
									return PrepareStatementFailed
								}
								field.SetUint(num)
							}
						case reflect.Int64:
							{
								num, err := strconv.ParseInt(arg, 10, 64)
								if err != nil {
									log.Println("Prepare statement: Failed to convert arg to int64")
									return PrepareStatementFailed
								}
								field.SetInt(num)
							}
						default:
							{
								log.Printf("Prepare statement: Not supported type: %v", field.Kind())
								return PrepareStatementFailed
							}
						}
					}
				}
			}
			stm.row = &row
		}
	case "select":
		{
			stm.typ = StatementTypeSelect
			row := &row.UserRow{}
			row.TableName = "User"
			stm.row = row
		}
	default:
		{
			stm.typ = StatementTypeInvalid
		}
	}

	return PrepareStatementSuccess
}
