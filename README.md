# Servidor de Dispositivos de Volta

[![Build Status](https://secure.travis-ci.org/alanxz/rabbitmq-c.png?branch=master)](http://travis-ci.org/alanxz/rabbitmq-c)

## main_volta.c

Archivo fuente principal del programa

## funciones.h

Archivo de cabecera donde se almacenan las funciones necesarias para apagar, encender, enclazar, etc.

## librabbit.a

Librería estática con los objetos de RabbitMQ, para poder compilar y ejecutar el servidor

## libjsmn.a

Librería estática con el parser de JSON

## caberecas

Carpeta que incluye las cabeceras de los objetos que hay en las librerías

## Instrucción de instalación

Necesario compilador gcc
Una vez estamos dentro de la carpeta del proyecto, ejecutar:
	gcc main_volta.c -o nombreejecutable -I./cabeceras -L./ -lrabbit -ljsmn

Si da algún tipo de problema el compilador:	

	gcc main_volta.c -o nombreejecutable -I./cabeceras -L./ -lrabbit -ljsmn -std=c99

Para poder ver los objetos que tiene las librerías:

	ar -t libjsmn.a
	ar -t librabbit.a

Para poder ver los archivos fuentes de la librería de RabbitMQ, consultar este proyecto:

	https://github.com/alanxz/rabbitmq-c#building-and-installing

Para poder ver los archivos fuentes de la librería del parser de JSON, consultar este proyecto:

	http://bitbucket.org/zserge/jsmn
