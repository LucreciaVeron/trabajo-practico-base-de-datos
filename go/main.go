package main

import (
	"database/sql"
	"fmt"
	"log"

	_ "github.com/lib/pq"
)

func crearDatabase() {
	db, err := sql.Open("postgres", "user=postgres host=localhost dbname=postgres sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	_, err = db.Exec(`drop database if exist tarjetasdecredito`)
	if err != nil {
		log.Fatal(err)
	}

	_, err = db.Exec(`create database tarjetasdecredito`)
	if err != nil {
		log.Fatal(err)
	}
}

func crearTablas() {
	db, err := sql.Open("postgres", "user=postgres host=localhost dbname=tarjetasdecredito sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	_, err = db.Exec(`create table cliente (nrocliente int, nombre text, apellido text, domicilio text, telefono char(12));
					create table tarjeta (nrotarjeta char(16), nrocliente int, validadesde char(6), validahasta char(6), codseguridad char(4), limitecompra decimal(8,2), estado char(10));
					create table comercio (nrocomercio int, nombre text, domicilio text, codigopostal char(8), telefono char(12));
					create table compra (nrooperacion int, nrotarjeta char(16), nrocomercio int, fecha timestamp, monto decimal(7,2), pagado boolean);
					create table rechazo (nrorechazo	int, nrotarjeta	char(16), nrocomercio int, fecha timestamp, monto decimal(7,2), motivo	text);
					create table cierre (ano int, mes int, terminacion int, fechainicio date,fechacierre date,fechavto date);
					create table cabecera (nroresumen	int, nombre text, apellido text, domicilio text, nrotarjeta char(16), desde date, hasta date, vence date, total decimal(8,2));
					create table detalle (nroresumen int, nrolinea int, fecha date, nombrecomercio text, monto decimal(7,2));
					create table alerta (nroalerta int, nrotarjeta char(16), fecha timestamp, nrorechazo int, codalerta int, descripcion text);
					create table consumo (nrotarjeta char(16), codseguridad char(4), nrocomercio int, monto decimal(7,2));`)
	if err != nil {
		log.Fatal(err)
	}
}

func crearPKS_FKS() {
	db, err := sql.Open("postgres", "user=postgres host=localhost dbname=tarjetasdecredito sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	//PKS
	_, err = db.Exec(`alter table cliente add constraint cliente_pk primary key (nrocliente);
					alter table tarjeta add constraint tarjeta_pk primary key (nrotarjeta);
					alter table comercio add constraint comercio_pk primary key (nrocomercio);
					alter table compra add constraint cliente_pk primary key (nrocliente);
					alter table rechazo add constraint rechazo_pk primary key (nrorechazo);
					alter table cierre add constraint cierre_pk primary key (ano , mes , terminacion);
					alter table cabecera add constraint cabecera_pk primary key (nroresumen);
					alter table detalle add constraint detalle_pk primary key (nroresumen, nrolinea);
					alter table alerta add constraint alerta_pk primary key (nroalerta);
					alter table tarjeta add constraint tarjeta_nrocliente_fk foreign key (nrocliente) references cliente(nrocliente);
					alter table compra add constraint compra_nrotarjeta_fk foreign key (nrotarjeta) references tarjeta(nrotarjeta);
					alter table compra add constraint compra_nrocomercio_fk foreign key (nrocomercio) references comercio(nrocomercio);
					alter table rechazo add constraint rechazo_nrocomercio_fk foreign key (nrocomercio) references comercio(nrocomercio);
					alter table cabecera add constraint cabecera_nrotarjeta_fk foreign key (nrotarjeta) references tarjeta(nrotarjeta);
					alter table detalle add constraint detalle_nroresumen_fk foreign key (nroresumen) references cabecera(nroresumen);
					alter table alerta add constraint alerta_nrotarjeta_fk foreign key (nrotarjeta) references tarjeta(nrotarjeta);
					alter table alerta add constraint alerta_nrorechazo_fk foreign key (nrorechazo) references rechazo(nrorechazo);
					alter table consumo add constraint consumo_nrotarjeta_fk foreign key (nrotarjeta) references tarjeta(nrotarjeta);
					alter table consumo add constraint consumo_nrocomercio_fk foreign key (nrocomercio) references comercio(nrocomercio);`)
	if err != nil {
		log.Fatal(err)
	}
}

// cargar clientes
func cargarClientes() {
	db, err := sql.Open("postgres", "user=postgres host=localhost dbname=tarjetasdecredito sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	_, err = db.Exec(`insert into cliente values (1,'federico','gonzalez','matoso 4',1123455252);
						insert into cliente values (2 ,'lucrecia','veron','av libertador 4567',1154568596);
						insert into cliente values (3,'martin','choque','juan b justo 3456',1123546895);
						insert into cliente values (4,'juan','leguizamon','monti 567',1111225544);
						insert into cliente values (5,'franco','elisei','buchiazzo 547',1123545687);
						insert into cliente values (6,'balbina','diaz','san martin 4356',1123548789);
						insert into cliente values (7,'amador','gonzalez','sourdeaux 254',1123541178);
						insert into cliente values (8,'pablo','perez','r.balbin 5456',1123548795);
						insert into cliente values (9,'marcela','rojas','gral pico 5456',1145568595);
						insert into cliente values (10,'pablo','russo','jose leon suares 5856',1154265845);
						insert into cliente values (11,'joshep','stalin','moscuo 8565',1154565212);
						insert into cliente values (12,'roberto','bolaños','mexico 5845',1123545623);
						insert into cliente values (13,'homer','simpson','siempre viva 724',115485698);
						insert into cliente values (14,'jana','maradona','marañon',1125563254);
						insert into cliente values (15,'guido','kazfca','palermo 4521',1125653242);
						insert into cliente values (16,'pedro','capussotto','av.locura 1235',1125454564);
						insert into cliente values (17,'herminio','igleas','monson 4526',1123547856);
						insert into cliente values (18,'carlos','garcia','puente puerredon 4523',1154854112);
						insert into cliente values (19,'david','linch','new york 452',1123568598);
						insert into cliente values (20,'guillermo','moreno','17 de octubre 1235',1125456523);`)
	if err != nil {
		log.Fatal(err)
	}

}

// cargar comercios
func cargarComercios() {
	db, err := sql.Open("postgres", "user=postgres host=localhost dbname=tarjetasdecredito sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	_, err = db.Exec(`insert into comercio values (1,'pollos hermanos','suprabon 1235','B4566',47411563); --mismo codigo postal que 1
						insert into comercio values(2,'crismar','maipu 4585','B4566',44635429); --mismo codigo postal que 1
						insert into comercio values(3,'la costera','italia 8007','C5256',48781525);
						insert into comercio values(4,'polinoa','francia 8075','B4585',44526985);-- mismo codigo postal que 5
						insert into comercio values(5,'el pez azul','panamericana 202','B4585',44856932);-- mismo codigo postal que 4
						insert into comercio values(6,'bola ocho','ruta 202 5245','V4565',47411515);
						insert into comercio values(7,'farmacity','cordoba 8585','C4556',44225485);
						insert into comercio values(8,'plusmar','talar 859','B1236',44859663);
						insert into comercio values(9,'boquita','rolon 4565','V5854',44123652);
						insert into comercio values(10,'tit can gross','australia 8552','A2522',45122365);
						insert into comercio values(11,'imperial','lavalle 3225','T1254',44115533);
						insert into comercio values(12,'la union basica','peron 4585','T4556',44255265);
						insert into comercio values(13,'rio verde','enriquez 2545','R2565',44788956);
						insert into comercio values(14,'gran valle','lanus 4565','T1225',42223365);
						insert into comercio values(15,'vegetalez','parana 4556','Y5262',44566235);
						insert into comercio values(16,'paty','ugarte 5254','P4556',44255632);
						insert into comercio values(17,'alto parana','lavalle 8888','C4885',42255663);
						insert into comercio values(18,'el general','montoya 4558','W1223',47778563);
						insert into comercio values(19,'los compañeros','evita 8556','E1223',48855223);
						insert into comercio values(20,'nini','estomba 4585','F4585',44566223);`)
	if err != nil {
		log.Fatal(err)
	}

}

// cargar tarjetas
func cargarTarjetas() {
	db, err := sql.Open("postgres", "user=postgres host=localhost dbname=tarjetasdecredito sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	_, err = db.Exec(`insert into tarjeta values('7496323374756950',1,'202001','202507','236',50000,'vigente');
						insert into tarjeta values('7326265148878640',2,'201802','202508','125',60000,'vigente');
						--insert into tarjeta values('7326265148878604',2,'201802','202208','155',60000,'vigente');--Prueba
						insert into tarjeta values('5459306968748220',3,'202005','202401','555',80000,'suspendida'); -- suspendida
						insert into tarjeta values('4214573590184040',4,'201805','202305','666',12000,'vigente');
						insert into tarjeta values('9196077466558780',5,'201906','202507','125',50000,'vigente');
						insert into tarjeta values('4934642269353250',6,'202105','202406','985',10000,'vigente');
						insert into tarjeta values('7238188501160730',7,'202203','202701','745',42000,'vigente');
						insert into tarjeta values('5749787089471770',8,'202004','202803','124',85000,'vigente');
						insert into tarjeta values('9826276300055020',9,'201903','202310','145',40000,'vigente');
						insert into tarjeta values('9322852380908470',10,'202201','202702','123',6000,'vigente');
						insert into tarjeta values('7877883759924660',11,'201501','202101','333',6000,'vigente'); --tarjeta vencida
						insert into tarjeta values('7987325800061700',12,'201903','202504','526',123000,'vigente');
						insert into tarjeta values('3242357542521350',13,'202004','202406','859',125000,'vigente');
						insert into tarjeta values('3437797611039410',14,'202005','202403','685',1200,'vigente');
						insert into tarjeta values('8055118055198750',15,'202106','202601','654',23000,'vigente');
						insert into tarjeta values('4050831930246320',16,'202201','202403','785',15000,'vigente');
						insert into tarjeta values('7893005661369480',17,'201903','202402','654',20000,'vigente');
						insert into tarjeta values('2880944801757820',18,'202004','202403','562',18000,'vigente');
						insert into tarjeta values('8656287980337840',19,'202108','202512','698',20000,'vigente');
						insert into tarjeta values('8056974961713030',20,'201865','202410','985',80000,'vigente');
						insert into tarjeta values('7651726189938030',11,'202102','202565','412',100000,'vigente');--segunda tarjeta del cliente 11 (de la vencida)
						insert into tarjeta values('2039608388720180',20,'201965','202411','456',15000,'anulada'); -- segunda tarjeta del cliente 20`)
	if err != nil {
		log.Fatal(err)
	}

}

// cargar cierres
func cargarCierres() {
	db, err := sql.Open("postgres", "user=postgres host=localhost dbname=tarjetasdecredito sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	_, err = db.Exec(`insert into cierre values(2022, 1, 0, '2022-01-01', '2022-02-01', '2022-02-05');
						insert into cierre values(2022, 2, 0, '2022-02-01', '2022-03-01', '2022-03-05');
						insert into cierre values(2022, 3, 0, '2022-03-01', '2022-04-01', '2022-04-05');
						insert into cierre values(2022, 4, 0, '2022-04-01', '2022-05-01', '2022-05-05');
						insert into cierre values(2022, 5, 0, '2022-05-01', '2022-06-01', '2022-06-05');
						insert into cierre values(2022, 6, 0, '2022-06-01', '2022-07-01', '2022-07-05');
						insert into cierre values(2022, 7, 0, '2022-07-01', '2022-08-01', '2022-08-05');
						insert into cierre values(2022, 8, 0, '2022-08-01', '2022-09-01', '2022-09-05');
						insert into cierre values(2022, 9, 0, '2022-09-01', '2022-10-01', '2022-10-05');
						insert into cierre values(2022, 10, 0, '2022-10-01', '2022-11-01', '2022-11-05');
						insert into cierre values(2022, 11, 0, '2022-11-01', '2022-12-01', '2022-12-05');
						insert into cierre values(2022, 12, 0, '2022-12-01', '2023-01-01', '2023-01-05');
						
						insert into cierre values(2022, 1, 1, '2022-01-02', '2022-02-02', '2022-02-06');
						insert into cierre values(2022, 2, 1, '2022-02-02', '2022-03-02', '2022-03-06');
						insert into cierre values(2022, 3, 1, '2022-03-02', '2022-04-02', '2022-04-06');
						insert into cierre values(2022, 4, 1, '2022-04-02', '2022-05-02', '2022-05-06');
						insert into cierre values(2022, 5, 1, '2022-05-02', '2022-06-02', '2022-06-06');
						insert into cierre values(2022, 6, 1, '2022-06-02', '2022-07-02', '2022-07-06');
						insert into cierre values(2022, 7, 1, '2022-07-02', '2022-08-02', '2022-08-06');
						insert into cierre values(2022, 8, 1, '2022-08-02', '2022-09-02', '2022-09-06');
						insert into cierre values(2022, 9, 1, '2022-09-02', '2022-10-02', '2022-10-06');
						insert into cierre values(2022, 10, 1, '2022-10-02', '2022-11-02', '2022-11-06');
						insert into cierre values(2022, 11, 1, '2022-11-02', '2022-12-02', '2022-12-06');
						insert into cierre values(2022, 12, 1, '2022-12-02', '2023-01-02', '2023-01-06');
						
						insert into cierre values(2022, 1, 2, '2022-01-03', '2022-02-03', '2022-02-07');
						insert into cierre values(2022, 2, 2, '2022-02-03', '2022-03-03', '2022-03-07');
						insert into cierre values(2022, 3, 2, '2022-03-03', '2022-04-03', '2022-04-07');
						insert into cierre values(2022, 4, 2, '2022-04-03', '2022-05-03', '2022-05-07');
						insert into cierre values(2022, 5, 2, '2022-05-03', '2022-06-03', '2022-06-07');
						insert into cierre values(2022, 6, 2, '2022-06-03', '2022-07-03', '2022-07-07');
						insert into cierre values(2022, 7, 2, '2022-07-03', '2022-08-03', '2022-08-07');
						insert into cierre values(2022, 8, 2, '2022-08-03', '2022-09-03', '2022-09-07');
						insert into cierre values(2022, 9, 2, '2022-09-03', '2022-10-03', '2022-10-07');
						insert into cierre values(2022, 10, 2, '2022-10-03', '2022-11-03', '2022-11-07');
						insert into cierre values(2022, 11, 2, '2022-11-03', '2022-12-03', '2022-12-07');
						insert into cierre values(2022, 12, 2, '2022-12-03', '2023-01-03', '2023-01-07');
						
						insert into cierre values(2022, 1, 3, '2022-01-04', '2022-02-04', '2022-02-08');
						insert into cierre values(2022, 2, 3, '2022-02-04', '2022-03-04', '2022-03-08');
						insert into cierre values(2022, 3, 3, '2022-03-04', '2022-04-04', '2022-04-08');
						insert into cierre values(2022, 4, 3, '2022-04-04', '2022-05-04', '2022-05-08');
						insert into cierre values(2022, 5, 3, '2022-05-04', '2022-06-04', '2022-06-08');
						insert into cierre values(2022, 6, 3, '2022-06-04', '2022-07-04', '2022-07-08');
						insert into cierre values(2022, 7, 3, '2022-07-04', '2022-08-04', '2022-08-08');
						insert into cierre values(2022, 8, 3, '2022-08-04', '2022-09-04', '2022-09-08');
						insert into cierre values(2022, 9, 3, '2022-09-04', '2022-10-04', '2022-10-08');
						insert into cierre values(2022, 10, 3, '2022-10-04', '2022-11-04', '2022-11-08');
						insert into cierre values(2022, 11, 3, '2022-11-04', '2022-12-04', '2022-12-08');
						insert into cierre values(2022, 12, 3, '2022-12-04', '2023-01-04', '2023-01-08');
						
						insert into cierre values(2022, 1, 4, '2022-01-05', '2022-02-05', '2022-02-09');
						insert into cierre values(2022, 2, 4, '2022-02-05', '2022-03-05', '2022-03-09');
						insert into cierre values(2022, 3, 4, '2022-03-05', '2022-04-05', '2022-04-09');
						insert into cierre values(2022, 4, 4, '2022-04-05', '2022-05-05', '2022-05-09');
						insert into cierre values(2022, 5, 4, '2022-05-05', '2022-06-05', '2022-06-09');
						insert into cierre values(2022, 6, 4, '2022-06-05', '2022-07-05', '2022-07-09');
						insert into cierre values(2022, 7, 4, '2022-07-05', '2022-08-05', '2022-08-09');
						insert into cierre values(2022, 8, 4, '2022-08-05', '2022-09-05', '2022-09-09');
						insert into cierre values(2022, 9, 4, '2022-09-05', '2022-10-05', '2022-10-09');
						insert into cierre values(2022, 10, 4, '2022-10-05', '2022-11-05', '2022-11-09');
						insert into cierre values(2022, 11, 4, '2022-11-05', '2022-12-05', '2022-12-09');
						insert into cierre values(2022, 12, 4, '2022-12-05', '2023-01-05', '2023-01-09');
						
						insert into cierre values(2022, 1, 5, '2022-01-06', '2022-02-06', '2022-02-10');
						insert into cierre values(2022, 2, 5, '2022-02-06', '2022-03-06', '2022-03-10');
						insert into cierre values(2022, 3, 5, '2022-03-06', '2022-04-06', '2022-04-10');
						insert into cierre values(2022, 4, 5, '2022-04-06', '2022-05-06', '2022-05-10');
						insert into cierre values(2022, 5, 5, '2022-05-06', '2022-06-06', '2022-06-10');
						insert into cierre values(2022, 6, 5, '2022-06-06', '2022-07-06', '2022-07-10');
						insert into cierre values(2022, 7, 5, '2022-07-06', '2022-08-06', '2022-08-10');
						insert into cierre values(2022, 8, 5, '2022-08-06', '2022-09-06', '2022-09-10');
						insert into cierre values(2022, 9, 5, '2022-09-06', '2022-10-06', '2022-10-10');
						insert into cierre values(2022, 10, 5, '2022-10-06', '2022-11-06', '2022-11-10');
						insert into cierre values(2022, 11, 5, '2022-11-06', '2022-12-06', '2022-12-10');
						insert into cierre values(2022, 12, 5, '2022-12-06', '2023-01-06', '2023-01-10');
						
						insert into cierre values(2022, 1, 6, '2022-01-07', '2022-02-07', '2022-02-11');
						insert into cierre values(2022, 2, 6, '2022-02-07', '2022-03-07', '2022-03-11');
						insert into cierre values(2022, 3, 6, '2022-03-07', '2022-04-07', '2022-04-11');
						insert into cierre values(2022, 4, 6, '2022-04-07', '2022-05-07', '2022-05-11');
						insert into cierre values(2022, 5, 6, '2022-05-07', '2022-06-07', '2022-06-11');
						insert into cierre values(2022, 6, 6, '2022-06-07', '2022-07-07', '2022-07-11');
						insert into cierre values(2022, 7, 6, '2022-07-07', '2022-08-07', '2022-08-11');
						insert into cierre values(2022, 8, 6, '2022-08-07', '2022-09-07', '2022-09-11');
						insert into cierre values(2022, 9, 6, '2022-09-07', '2022-10-07', '2022-10-11');
						insert into cierre values(2022, 10, 6, '2022-10-07', '2022-11-07', '2022-11-11');
						insert into cierre values(2022, 11, 6, '2022-11-07', '2022-12-07', '2022-12-11');
						insert into cierre values(2022, 12, 6, '2022-12-07', '2023-01-07', '2023-01-11');
						
						insert into cierre values(2022, 1, 7, '2022-01-08', '2022-02-08', '2022-02-12');
						insert into cierre values(2022, 2, 7, '2022-02-08', '2022-03-08', '2022-03-12');
						insert into cierre values(2022, 3, 7, '2022-03-08', '2022-04-08', '2022-04-12');
						insert into cierre values(2022, 4, 7, '2022-04-08', '2022-05-08', '2022-05-11');
						insert into cierre values(2022, 5, 7, '2022-05-08', '2022-06-08', '2022-06-11');
						insert into cierre values(2022, 6, 7, '2022-06-08', '2022-07-08', '2022-07-12');
						insert into cierre values(2022, 7, 7, '2022-07-08', '2022-08-08', '2022-08-12');
						insert into cierre values(2022, 8, 7, '2022-08-08', '2022-09-08', '2022-09-12');
						insert into cierre values(2022, 9, 7, '2022-09-08', '2022-10-08', '2022-10-12');
						insert into cierre values(2022, 10, 7, '2022-10-08', '2022-11-08', '2022-11-12');
						insert into cierre values(2022, 11, 7, '2022-11-08', '2022-12-08', '2022-12-12');
						insert into cierre values(2022, 12, 7, '2022-12-08', '2023-01-08', '2023-01-12');
						
						insert into cierre values(2022, 1, 8, '2022-01-09', '2022-02-09', '2022-02-13');
						insert into cierre values(2022, 2, 8, '2022-02-09', '2022-03-09', '2022-03-13');
						insert into cierre values(2022, 3, 8, '2022-03-09', '2022-04-09', '2022-04-13');
						insert into cierre values(2022, 4, 8, '2022-04-09', '2022-05-09', '2022-05-13');
						insert into cierre values(2022, 5, 8, '2022-05-09', '2022-06-09', '2022-06-13');
						insert into cierre values(2022, 6, 8, '2022-06-09', '2022-07-09', '2022-07-13');
						insert into cierre values(2022, 7, 8, '2022-07-09', '2022-08-09', '2022-08-13');
						insert into cierre values(2022, 8, 8, '2022-08-09', '2022-09-09', '2022-09-13');
						insert into cierre values(2022, 9, 8, '2022-09-09', '2022-10-09', '2022-10-13');
						insert into cierre values(2022, 10, 8, '2022-10-09', '2022-11-09', '2022-11-13');
						insert into cierre values(2022, 11, 8, '2022-11-09', '2022-12-09', '2022-12-13');
						insert into cierre values(2022, 12, 8, '2022-12-09', '2023-01-09', '2023-01-13');
						
						insert into cierre values(2022, 1, 9, '2022-01-10', '2022-02-10', '2022-02-14');
						insert into cierre values(2022, 2, 9, '2022-02-10', '2022-03-10', '2022-03-14');
						insert into cierre values(2022, 3, 9, '2022-03-10', '2022-04-10', '2022-04-14');
						insert into cierre values(2022, 4, 9, '2022-04-10', '2022-05-10', '2022-05-14');
						insert into cierre values(2022, 5, 9, '2022-05-10', '2022-06-10', '2022-06-14');
						insert into cierre values(2022, 6, 9, '2022-06-10', '2022-07-10', '2022-07-14');
						insert into cierre values(2022, 7, 9, '2022-07-10', '2022-08-10', '2022-08-14');
						insert into cierre values(2022, 8, 9, '2022-08-10', '2022-09-10', '2022-09-14');
						insert into cierre values(2022, 9, 9, '2022-09-10', '2022-10-10', '2022-10-14');
						insert into cierre values(2022, 10, 9, '2022-10-10', '2022-11-10', '2022-11-14');
						insert into cierre values(2022, 11, 9, '2022-11-10', '2022-12-10', '2022-12-14');
						insert into cierre values(2022, 12, 9, '2022-12-10', '2023-01-10', '2023-01-14');`)
	if err != nil {
		log.Fatal(err)
	}
}

func cargarTablas() {
	db, err := sql.Open("postgres", "user=postgres host=localhost dbname=tarjetasdecredito sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	cargarTarjetas()
	cargarClientes()
	cargarComercios()
}

func crearStoredProceduresYTriggers() {
	db, err := sql.Open("postgres", "user=postgres host=localhost dbname=tarjetasdecredito sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	_, err = db.Exec(`
	create or replace function borraPKsyFKs() returns void as $$
begin
	alter table tarjeta drop constraint tarjeta_nrocliente_fk;
	alter table compra drop constraint compra_nrotarjeta_fk;
	alter table compra drop constraint compra_nrocomercio_fk;
	--alter table rechazo drop constraint rechazo_nrotarjeta_fk;
	alter table rechazo drop constraint rechazo_nrocomercio_fk;
	alter table cabecera drop constraint cabecera_nrotarjeta_fk;
	alter table detalle drop constraint detalle_nroresumen_fk;
	alter table alerta drop constraint alerta_nrotarjeta_fk;
	alter table alerta drop constraint alerta_nrorechazo_fk;
	alter table consumo drop constraint consumo_nrotarjeta_fk;
	alter table consumo drop constraint consumo_nrocomercio_fk;

	alter table cliente drop constraint cliente_pk;
	alter table tarjeta drop constraint tarjeta_pk;
	alter table comercio drop constraint comercio_pk;
	alter table compra drop constraint compra_pk;
	alter table rechazo drop constraint rechazo_pk;
	alter table cierre drop constraint cierre_pk;
	alter table cabecera drop constraint cabecera_pk;
	alter table detalle drop constraint detalle_pk;
	alter table alerta drop constraint alerta_pk;

end;
$$ language plpgsql;

--consumos
create or replace function cargarConsumos() returns void as $$
begin
insert into consumo values ('4050831930246320','785',1,45);
insert into consumo values ('8656287980337840','698',2,22000); --excede limite
insert into consumo values ('8656287980337840','698',2,22000); -- excede el limite por segunda vez
insert into consumo values ('7877883759924660','333',3,4000); -- tarjeta vencida
insert into consumo values ('9196077466558780','125',4,3000); -- 2 compras en distintos locales con mismo codigo postal alerta 1 min
insert into consumo values ('9196077466558780','125',5,2500);--2 compras en distintos locales con mismo codigo postal alerta 1 min
insert into consumo values ('9196077466558780','125',6,2500); -- 2 compras en distintos cod postales con menos de 5 minutos
insert into consumo values ('9196077466558780','125',7,2500); -- 2 compras en distintos cod postales con menos de 5 minutos
insert into consumo values ('2880944801757820','000',11,500); -- codigo de seguridad erroneo
insert into consumo values ('4050831930246320','785',2,45); --distintos comercios mismo codigo postal mismo codigo postal alerta 1 min
insert into consumo values ('4050831930246320','785',1,45); --distintos comercios mismo codigo postal mismo codigo postal alerta 1 min
insert into consumo values ('5459306968748220','555',12,100); -- tarjeta suspendida
insert into consumo values ('2039608388720180','456',15,15); -- tarjeta anulada
insert into consumo values ('5749787089471770','124',5,500); 
insert into consumo values ('8056974961713030','985',8,250);

end;
$$language plpgsql;

create or replace function convertir_fecha() returns int as $$
declare
	aux char(10);
	auxx char(6);
	auxxx int;
	
begin
	aux := cast(current_date as char(10));
	auxx := concat(substring(aux from 1 for 4), substring(aux from 6 for 2));
	auxxx := cast(auxx as int);
	return auxxx;
end;
$$ language plpgsql;

--Stored Procedures y Triggers
--Autorizacion compra
create or replace function autorizacion_compra(nro_tarjeta char(16), cod_seguridad char(4), num_comercio int, montoo decimal(7,2)) returns boolean as $$  
declare
	tarjeta_buscada record;
	compra_autorizada boolean;
	suma_compras decimal(7,2);
	nro_rechazo int;
	nro_operacion int;
	fecha_actual timestamp;
	
begin
	select count(*) into nro_rechazo from rechazo;
	nro_rechazo := nro_rechazo + 1;
	
	select * into tarjeta_buscada from tarjeta where nrotarjeta = nro_tarjeta;
	if not found then
		fecha_actual := current_timestamp(0);
		insert into rechazo values(nro_rechazo, nro_tarjeta, num_comercio, fecha_actual, montoo, 'Tarjeta no valida');
		return false;
	end if;
	

	if tarjeta_buscada.estado = 'anulada' then
		fecha_actual := current_timestamp(0);
		insert into rechazo values(nro_rechazo, nro_tarjeta, num_comercio, fecha_actual, montoo, 'Tarjeta no valida');
		return false;
		
	elsif tarjeta_buscada.estado = 'suspendida' then
		fecha_actual := current_timestamp(0);
		insert into rechazo values (nro_rechazo, nro_tarjeta, num_comercio, fecha_actual, montoo, 'La tarjeta se encuentra suspendida');
		return false;

	elsif tarjeta_buscada.codseguridad != cod_seguridad then
		fecha_actual := current_timestamp(0);
		insert into rechazo values(nro_rechazo, nro_tarjeta, num_comercio, fecha_actual, montoo, 'Codigo de seguridad invalido');
		return false;
	end if;
	
	select sum (monto) into suma_compras from compra where nrotarjeta = nro_tarjeta and pagado = false;	--Sumamos aquellas compras que fueron hechas con dicha tarjeta y que no esten pagas
	if not found then
		suma_compras := suma_compras + montoo;
	elsif found then
		suma_compras := montoo;
	end if;
	
	if tarjeta_buscada.limitecompra < suma_compras then
		fecha_actual := current_timestamp(0);
		insert into rechazo values(nro_rechazo, nro_tarjeta, num_comercio, fecha_actual, montoo, 'Supera limite de tarjeta');
		return false;
	end if;
	
	if cast(tarjeta_buscada.validahasta as int) < convertir_fecha() then
		fecha_actual := current_timestamp(0);
		insert into rechazo values(nro_rechazo, nro_tarjeta, num_comercio, fecha_actual, montoo, 'Plazo de vigencia expirado');
		return false;
	end if;

	select count(*) into nro_operacion from compra;
	nro_operacion = nro_operacion + 1;
	fecha_actual := current_timestamp(0);
	insert into compra values(nro_operacion, nro_tarjeta, num_comercio, fecha_actual, montoo, false);
	return true;

end;
$$ language plpgsql;

--autorizacion general
create or replace function autorizar_todas_las_compras () returns void as $$
	declare
	consumos record;
	resultado boolean;
	begin
	for consumos in select * from consumo loop
		resultado := autorizacion_compra (consumos.nrotarjeta, consumos.codseguridad, consumos.nrocomercio, consumos.monto);
		if resultado then
		raise notice 'transaccion realiza con exito';
		else
		raise notice ' la transaccion no pudo realizarse';
		end if;
	end loop;
	truncate table consumo;
end;
$$ language plpgsql;



		
--Generar resumen de la tarjeta
create or replace function generacion_cabecera(nro_cliente int, periodo int) returns void as $$
declare
	cliente_buscado record; --cliente
	tarjeta_cliente record; --tarjeta
	compra_cliente record; --compra
	total_pagar decimal(8,2);
	cierre_tarjeta record; --cierre
	nro_resumen int;
	
	fecha_desde date;
	fecha_hasta date;
	fecha_vencimiento date;

begin
		total_pagar := 0;
	for tarjeta_cliente in select * from tarjeta where nrocliente = nro_cliente loop

		--Fechas del cierre de la tarjeta para añadir en la cabecera
		select * into cierre_tarjeta from cierre where mes = periodo and terminacion = substring(tarjeta_cliente.nrotarjeta, length(tarjeta_cliente.nrotarjeta))::int;
		fecha_desde = cierre_tarjeta.fechainicio; --date
		fecha_hasta = cierre_tarjeta.fechacierre; -- date
		fecha_vencimiento = cierre_tarjeta.fechavto; --date

		select * into cliente_buscado from cliente where nrocliente = nro_cliente;


		--Buscamos las compras que se hicieron con la tarjeta del cliente
		for compra_cliente in select * from compra where nrotarjeta = tarjeta_cliente.nrotarjeta loop
		 if extract (month from compra_cliente.fecha) = periodo then
			total_pagar := total_pagar + compra_cliente.monto;
			end if;
		end loop;
		
		select count(*) into nro_resumen from cabecera;
		nro_resumen = nro_resumen + 1;
		insert into cabecera values (nro_resumen, cliente_buscado.nombre, cliente_buscado.apellido, cliente_buscado.domicilio, tarjeta_cliente.nrotarjeta, fecha_desde, fecha_hasta, fecha_vencimiento, total_pagar);

		--Generamos el detalle de la tarjeta
		perform generacion_detalle(tarjeta_cliente.nrotarjeta, periodo);
	end loop;
end;
$$ language plpgsql;

create or replace function generacion_detalle(nro_tarjeta char(16), periodo int) returns void as $$
declare
	tarjeta_buscada record;
	nro_resumen int;
	nro_linea int;
	compras record;
	comercio_nombre text;
	mes int;
	
begin
	select nroresumen into nro_resumen from cabecera where nrotarjeta = nro_tarjeta;
	select * into tarjeta_buscada from tarjeta where nrotarjeta = nro_tarjeta;
	nro_linea := 1;
	
	--Compras realizadas con dicha tarjeta
	for compras in select * from compra where nrotarjeta = tarjeta_buscada.nrotarjeta loop
		--Se buscan las compras que se realizaron en el mes
		mes := cast((select extract(month from compras.fecha)) as int);
		if mes = periodo then
			select nombre into comercio_nombre from comercio where nrocomercio = compras.nrocomercio;
			insert into detalle values(nro_resumen, nro_linea, compras.fecha, comercio_nombre, compras.monto);
			nro_linea := nro_linea + 1;
		end if;
	end loop;
end;
$$ language plpgsql;

--Alertas a clientes

create or replace function chequearMinutos(nro_tarjeta char(16)) returns void as $$ --hace insert en rechazo
declare
	primeraCompra record;
	segundaCompra record;

	diferenciaMinutos interval; --esta de mas?
	cp_primerComercio char(8);
	cp_segundoComercio char(8);
	nro_alerta int;
	corte boolean;
	
	cincoMin interval;
	unMin interval;
	diferenciaFechas interval;

begin
	unMin := '1 minute'::interval;
	cincoMin := '5 minute'::interval;
	
	for primeraCompra in select * from compra where nrotarjeta = nro_tarjeta loop
		select codigopostal into cp_primerComercio from comercio where nrocomercio = primeraCompra.nrocomercio;
		for segundaCompra in select * from compra where nrotarjeta = nro_tarjeta loop
			select codigopostal into cp_segundoComercio from comercio where nrocomercio = segundaCompra.nrocomercio;
			
			diferenciaFechas := (segundaCompra.fecha - primeraCompra.fecha);

			if primeraCompra.nrocomercio != segundaCompra.nrocomercio and cp_primerComercio = cp_segundoComercio and diferenciaFechas < unMin and alerta_inexistente(nro_tarjeta,1) then
				select count(*) into nro_alerta from alerta;
				nro_alerta := nro_alerta + 1;
				insert into alerta values(nro_alerta, nro_tarjeta, current_timestamp, null,1, 'Se generaron 2 compras en menos de un minuto en diferentes comercios');
			elseif cp_primerComercio != cp_segundoComercio and diferenciaFechas < cincoMin and alerta_inexistente(nro_tarjeta,5) then
				select count(*) into nro_alerta from alerta;
				nro_alerta := nro_alerta + 1;
				insert into alerta values(nro_alerta, nro_tarjeta, current_timestamp, null,5, 'Se generaron 2 compras en menos de cinco minutos en distintas zonas');
			end if;
		
		end loop;
	end loop;
end;
$$ language plpgsql;

create function alerta_inexistente (nro_tarjeta char(16), cod_alerta int) returns boolean as $$ -- devuelve true sino existe esa tarjeta con esa alarta
declare
	alerta_buscada record;
begin
	select * into alerta_buscada from alerta where nrotarjeta = nro_tarjeta and codalerta = cod_alerta;
	if not found then
		return true;
	else
		return false;
	end if;
end;
$$ language plpgsql;

create function funcion_2_rechazos (nro_tarjeta char(16)) returns void as $$   --hace insert en rechazo
declare
	rechazos_1 record;
	rechazos_2 record;

	dia1 int;
	mes1 int;
	año1 int;

	dia2 int;
	mes2 int;
	año2 int;

	num_rechazo1 int;
	num_rechazo2 int;
	
	nro_alerta int;

begin

	for rechazos_1 in select * from rechazo where nrotarjeta = nro_tarjeta loop  -- ver todos los rechazos de una tarjeta
		dia1 := extract(day from rechazos_1.fecha);	
		mes1 := extract(month from rechazos_1.fecha);	
		año1 := extract(year from rechazos_1.fecha);	

		num_rechazo1 := rechazos_1.nrorechazo;

		for rechazos_2 in select * from rechazo where nrotarjeta = nro_tarjeta loop  
			dia2 := extract(day from rechazos_2.fecha);	
			mes2 := extract(month from rechazos_2.fecha);	
			año2 := extract(year from rechazos_2.fecha);	

			num_rechazo2 := rechazos_2.nrorechazo;

			--verificamos que la fecha sea la misma, sean rechazos distintos y tengan el mismo motivo
			if dia1 = dia2 and mes1 = mes2 and año1 = año2 and num_rechazo1 != num_rechazo2 and rechazos_2.motivo ='Supera limite de tarjeta' and rechazos_1.motivo = 'Supera limite de tarjeta'  and alerta_inexistente(nro_tarjeta,32)then
				select count(*) into nro_alerta from alerta;
				nro_alerta := nro_alerta + 1;
				insert into alerta values(nro_alerta ,nro_tarjeta,current_timestamp,null,32,'supera 2 veces el limite de la tarjeta en un mismo dia');
				update tarjeta set estado = 'suspendida' where nrotarjeta = nro_tarjeta;
			end if;
		end loop;
	end loop;
end;
$$ language plpgsql;

create or replace function funcion_alerta_rechazo() returns trigger as $$  --hace insert en rechazo
declare
rechazos record;
contAlertas int;

begin
	
	select count(*) into contAlertas from alerta;
	contAlertas := contAlertas + 1;
	insert into alerta values(contAlertas, new.nrotarjeta, new.fecha, new.nrorechazo, 0, new.motivo);
	perform funcion_2_rechazos(new.nrotarjeta);
	return new;
end;
$$ language plpgsql;

create or replace function funcion_alerta_rechazo_minutos() returns trigger as $$
begin
	perform chequearMinutos(new.nrotarjeta);
	return new;
end;
$$ language plpgsql;

create trigger alerta_rechazo
after insert on rechazo
for each row
execute procedure funcion_alerta_rechazo();


create trigger alerta_rechazo_minutos
after insert on compra
for each row
execute procedure funcion_alerta_rechazo_minutos();
	`)
	if err != nil {
		log.Fatal(err)
	}

}

func main() {
	db, err := sql.Open("postgres", "user=postgres host=localhost dbname=tarjetasdecredito sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	var eleccion int

	for true {
		fmt.Print("Por favor, ingrese una de las siguientes opciones:\n")
		fmt.Print("0 -- Salir\n")
		fmt.Print("1 -- Crear Base de Datos \n")
		fmt.Print("2 -- Crear Tablas\n")
		fmt.Print("3 -- Crear Pks y Fks\n")
		fmt.Print("4 -- Eliminar Pks y Fks\n")
		fmt.Print("5 -- Cargar tablas\n")
		fmt.Print("6 -- Crear Stored Procedures y Triggers\n")
		fmt.Print("7 -- Cargar cierres\n")
		fmt.Print("8 -- Generar consumos\n")
		fmt.Print("9 -- Autorizar compras\n")
		fmt.Print("10 -- Generar resumen\n")
		fmt.Scanf("%d", &eleccion)

		if eleccion == 0 {
			break
		}

		if eleccion == 1 {
			crearDatabase()
			fmt.Printf("Se creo la base de datos\n\n")
		}

		if eleccion == 2 {
			crearTablas()
			fmt.Printf("Se crearon las tablas\n\n")
		}

		if eleccion == 3 {
			crearPKS_FKS()
			fmt.Printf("Se crearon las PK's y FK's\n\n")
		}

		if eleccion == 4 {
			_, err = db.Exec("select borraPKsyFKs();")

			if err != nil {
				log.Fatal(err)
			}

			fmt.Printf("Se eliminarion las Pks y Fks\n\n")
		}

		if eleccion == 5 {
			cargarTablas()
			fmt.Printf("Se cargaron las tablas\n\n")
		}

		if eleccion == 6 {
			crearStoredProceduresYTriggers()
			fmt.Printf("Se crearon los Stored procedures y Triggers\n\n")
		}

		if eleccion == 7 {
			cargarCierres()
			fmt.Printf("Se cargaron los cierres\n\n")
		}

		if eleccion == 8 {
			_, err = db.Exec("select cargarConsumos();")
			if err != nil {
				log.Fatal(err)
			}

			fmt.Printf("Se generaron consumos\n\n")
		}

		if eleccion == 9 {
			_, err = db.Exec("select autorizar_todas_las_compras();")
			if err != nil {
				log.Fatal(err)
			}

			fmt.Printf("Se autorizaron las compras\n\n")
		}

		if eleccion == 10 {
			_, err = db.Exec("select generacion_cabecera(5,11);")
			if err != nil {
				log.Fatal(err)
			}

			fmt.Printf("El resumen del cliente 5 en el mes 11 fue generado\n\n")
		}
	}

}
