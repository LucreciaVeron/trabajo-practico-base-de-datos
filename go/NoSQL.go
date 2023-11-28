package main

import (
	"encoding/json"
	"log"

	"strconv"

	bolt "go.etcd.io/bbolt"
)

type Cliente struct {
	Nrocliente int
	Nombre     string
	Apellido   string
	Domicilio  string
	Telefono   string
}

type Tarjeta struct {
	Nrotarjeta   string
	Nrocliente   int
	Validadesde  string
	Validahasta  string
	Codseguridad string
	Limitecompra float64
	Estado       string
}

type Comercio struct {
	Nrocomercio  int
	Nombre       string
	Domicilio    string
	Codigopostal string
	Telefono     string
}

type Compra struct {
	Nrooperacion int
	Nrotarjeta   string
	Nrocomercio  int
	Fecha        string
	Monto        float64
	Pagado       bool
}

func CreateUpdate(db *bolt.DB, bucketName string, key []byte, val []byte) error {
	// abre transacción de escritura
	tx, err := db.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	b, _ := tx.CreateBucketIfNotExists([]byte(bucketName))

	err = b.Put(key, val)
	if err != nil {
		return err
	}

	// cierra transacción
	if err := tx.Commit(); err != nil {
		return err
	}

	return nil
}

func ReadUnique(db *bolt.DB, bucketName string, key []byte) ([]byte, error) {
	var buf []byte

	// abre una transacción de lectura
	err := db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucketName))
		buf = b.Get(key)
		return nil
	})

	return buf, err
}

func main() {
	db, err := bolt.Open("NoSQL.db", 0600, nil)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	//Guardamos datos para clientes
	cliente_1 := Cliente{10, "pablo", "russo", "jose leon suares 5856", "1154265845"}
	data1, err := json.Marshal(cliente_1)
	if err != nil {
		log.Fatal(err)
	}

	CreateUpdate(db, "Cliente", []byte(strconv.Itoa(cliente_1.Nrocliente)), data1)

	cliente_2 := Cliente{11, "joshep", "stalin", "moscuo 8565", "1154565212"}
	data2, err := json.Marshal(cliente_2)
	if err != nil {
		log.Fatal(err)
	}

	CreateUpdate(db, "Cliente", []byte(strconv.Itoa(cliente_2.Nrocliente)), data2)

	cliente_3 := Cliente{12, "roberto", "bolaños", "mexico 5845", "1123545623"}
	data3, err := json.Marshal(cliente_3)
	if err != nil {
		log.Fatal(err)
	}

	CreateUpdate(db, "Cliente", []byte(strconv.Itoa(cliente_3.Nrocliente)), data3)

	//Guardamos datos para tarjetas
	tarjeta_1 := Tarjeta{"9322852380908470", 10, "202201", "202702", "123", 6000, "vigente"}
	data4, err := json.Marshal(tarjeta_1)
	if err != nil {
		log.Fatal(err)
	}

	CreateUpdate(db, "Tarjeta", []byte(tarjeta_1.Nrotarjeta), data4)

	tarjeta_2 := Tarjeta{"7877883759924660", 11, "201501", "202101", "333", 6000, "anulada"}
	data5, err := json.Marshal(tarjeta_2)
	if err != nil {
		log.Fatal(err)
	}

	CreateUpdate(db, "Tarjeta", []byte(tarjeta_2.Nrotarjeta), data5)

	tarjeta_3 := Tarjeta{"7987325800061700", 12, "201903", "202504", "526", 123000, "vigente"}
	data6, err := json.Marshal(tarjeta_3)
	if err != nil {
		log.Fatal(err)
	}

	CreateUpdate(db, "Tarjeta", []byte(tarjeta_3.Nrotarjeta), data6)

	//Guardamos datos para comercio
	comercio_1 := Comercio{1, "pollos hermanos", "suprabon 1235", "B4566", "47411563"}
	data7, err := json.Marshal(comercio_1)
	if err != nil {
		log.Fatal(err)
	}

	CreateUpdate(db, "Comercio", []byte(strconv.Itoa(comercio_1.Nrocomercio)), data7)

	comercio_2 := Comercio{2, "crismar", "maipu 4585", "X5456", "44635429"}
	data8, err := json.Marshal(comercio_2)
	if err != nil {
		log.Fatal(err)
	}

	CreateUpdate(db, "Comercio", []byte(strconv.Itoa(comercio_2.Nrocomercio)), data8)

	comercio_3 := Comercio{3, "la costera", "italia 8007", "C5256", "48781525"}
	data9, err := json.Marshal(comercio_3)
	if err != nil {
		log.Fatal(err)
	}

	CreateUpdate(db, "Comercio", []byte(strconv.Itoa(comercio_3.Nrocomercio)), data9)

	//Guardamos datos para compras
	compra_1 := Compra{1, tarjeta_1.Nrotarjeta, comercio_1.Nrocomercio, "2022-06-21", 1580, true}
	data10, err := json.Marshal(compra_1)
	if err != nil {
		log.Fatal(err)
	}

	CreateUpdate(db, "Compra", []byte(strconv.Itoa(compra_1.Nrooperacion)), data10)

	compra_2 := Compra{2, tarjeta_2.Nrotarjeta, comercio_2.Nrocomercio, "2022-08-01", 562, true}
	data11, err := json.Marshal(compra_2)
	if err != nil {
		log.Fatal(err)
	}

	CreateUpdate(db, "Compra", []byte(strconv.Itoa(compra_2.Nrooperacion)), data11)

	compra_3 := Compra{3, tarjeta_3.Nrotarjeta, comercio_3.Nrocomercio, "2022-02-11", 1040, true}
	data12, err := json.Marshal(compra_3)
	if err != nil {
		log.Fatal(err)
	}

	CreateUpdate(db, "Compra", []byte(strconv.Itoa(compra_3.Nrooperacion)), data12)

	/*ejemplo_resultado, err := ReadUnique(db, "Compra", []byte(strconv.Itoa(compra_1.Nrooperacion)))
	fmt.Printf("%s\n", ejemplo_resultado)*/

}
