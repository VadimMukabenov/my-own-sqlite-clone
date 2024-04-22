package core

import (
	"log"
	"strconv"
)

func (stm *Statement) Execute() {
	switch stm.typ {
	case StatementTypeSelect:
		{
			runSelect(stm)
		}
	case StatementTypeInsert:
		{
			runInsert(stm)
		}
	case StatementTypeInvalid:
		{
			log.Println("Execute statement error: Invalid statement type")
		}
	default:
		{
			log.Printf("Execute statement error: Unhandled statement type: %v\n", stm.typ)
		}
	}
}

func runSelect(stm *Statement) {
	index, err := strconv.ParseUint(stm.args[1], 10, 64)
	if err != nil {
		log.Printf("Failed to run select, error parsing row index: %s\n", err)
	}
	stm.row.InitCursor(uint32(index))
	err = stm.row.Load()
	if err != nil {
		log.Printf("Failed to run select, error loading data: %s\n", err)
	}
}

func runInsert(stm *Statement) {
	index, err := strconv.ParseUint(stm.args[1], 10, 64)
	if err != nil {
		log.Fatalf("Failed to parse id: %s", err.Error())
	}
	n, err := stm.row.Save(uint32(index))
	if err != nil {
		log.Printf("Failed to run insert: %s", err.Error())
		return
	}
	log.Printf("Inserted %d bytes to table %s\n", n, stm.row.Table().String())
}
