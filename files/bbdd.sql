
#drop schema dam2tm06uf2p2;

create schema dam2tm06uf2p2;
use dam2tm06uf2p2;

create table Entrada (
	id int primary key auto_increment,
    instruccion varchar(100)
);

insert into Entrada (instruccion) values ('B Bodega1');
insert into Entrada (instruccion) values ('C');
insert into Entrada (instruccion) values ('V BLANCA 3');
insert into Entrada (instruccion) values ('V NEGRA 2');
insert into Entrada (instruccion) values ('B Bodega2');
insert into Entrada (instruccion) values ('C');
insert into Entrada (instruccion) values ('V Negra 4');
insert into Entrada (instruccion) values ('V Negra 1');
insert into Entrada (instruccion) values ('V blanCA 2');
insert into Entrada (instruccion) values ('#');
insert into Entrada (instruccion) values ('B bodega3');
insert into Entrada (instruccion) values ('B bodega4');
insert into Entrada (instruccion) values ('C');
insert into Entrada (instruccion) values ('B bodega5');
insert into Entrada (instruccion) values ('V Negra 4');
insert into Entrada (instruccion) values ('V blanCa 2');
insert into Entrada (instruccion) values ('V neGra 4');
insert into Entrada (instruccion) values ('#');
insert into Entrada (instruccion) values ('V neGRA 2');
insert into Entrada (instruccion) values ('V blanca 4');
insert into Entrada (instruccion) values ('C');
insert into Entrada (instruccion) values ('C');
insert into Entrada (instruccion) values ('V blanca 4');
insert into Entrada (instruccion) values ('#');
