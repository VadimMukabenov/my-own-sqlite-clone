package repl

import (
	"bufio"
	"fmt"
	"log"
	"my-cstack-db/src/core"
	"os"
	"strings"
)

func Run() {
	for {
		fmt.Print("db > ")

		reader := bufio.NewReader(os.Stdin)

		str, err := reader.ReadString('\n')
		if err != nil {
			os.Exit(1)
		}

		// \n was included in reader.ReadString
		// TODO Handle complex invalid input
		str = strings.TrimSpace(str)

		ib := core.InputBuffer{Args: strings.Split(str, " ")}

		op := ib.Args[0]
		if len(op) == 0 {
			continue
		}
		if op[0] == '.' {
			executeMetaCmd(&ib)
		} else {
			stm := core.Statement{}
			prepareStatus := core.PrepareStm(&ib, &stm)
			if prepareStatus == core.PrepareStatementFailed {
				log.Printf("Failed to prepare statement: %v\n", ib.Args)
				continue
			}
			stm.Execute()
		}
	}
}
