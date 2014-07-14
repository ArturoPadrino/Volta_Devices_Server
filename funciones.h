//
//  funciones.h
//  VoltaServer
//
//  Created by Arturo Padrino Vilela
//  Copyright (c) 2014 ArturoPadrino. All rights reserved.
//

#include "stdlib.h"
#include "stdio.h"
#include "string.h"
#include "stdint.h"
#include "amqp_tcp_socket.h"
#include "amqp.h"
#include "amqp_framing.h"
#include "assert.h"
#include "utils.h"
#include "jsmn.h"
#ifndef VoltaServer_funciones_h
#define VoltaServer_funciones_h
#define SUMMARY_EVERY_US 1000000

//Funcion para comparar un string con el contenido de un token de JSON
#define comparar_cadena_token(js, t, s) \
(strncmp(js+(t).start, s, (t).end - (t).start) == 0 \
&& strlen(s) == (t).end - (t).start)

void hallar_mac (int, char *);
void mandar_logs(int, int);
void gestion_mensajes2 (int, char*);
void run (amqp_connection_state_t,int, char*);
char *json_parser (char*, char*);
void comprobar_encendido (char*, int, int, amqp_connection_state_t);
void comprobar_apagado (char*, int, int, amqp_connection_state_t);
void comprobar_enlazado (char*, int, int, amqp_connection_state_t);
void mandar_respuesta(char*, char*, char*, char*, char*, amqp_connection_state_t);
#endif


//Funcion que halla la MAC del dispositivo, para poder usarla a posteriori
void hallar_mac(int newsockfd, char *mac){

	int bucle=1;
	char men_log[10000]="";
    /* Inicializacion de la cabecera de los mensajes de log */
    strcat (men_log,"::DEV::");
    strcat(men_log, getenv("PWD"));
    strcat(men_log, "::-");
	
	char buffer[10000], transfer[100000];
	int n;
	while (bucle==1) {
		memset(buffer, 0, 10000);
        //bzero(buffer,10000);
		//Leemos del socket
        n = read(newsockfd,buffer,9999);
        if (n < 0){
            openlog (men_log, 0, LOG_LOCAL4);
            syslog(LOG_ERR, "Error al leer del socket\n Archivo:funciones.h \nFuncion: hallar_mac");
            closelog();
            exit(1);
        }
        strcat(transfer, buffer);
		if (strstr(transfer,"</frm>")!=NULL) {
		
		
        //if ((transfer[strlen(transfer)-1]) == '\n'){
			
			openlog (men_log, 0, LOG_LOCAL5);
			syslog (LOG_INFO, "%s",transfer);
			closelog();
			//Leemos la MAC
            for (int i=10; i<=21; i++) {
                *mac=transfer[i];
                mac++;
            }
			
			bucle=0;
        }
    }
	
	*mac='\0';


}


/* Esta funcion se encarga de recibir los datos del gateway
 y de enviarlos a syslog */
void mandar_logs(int newsockfd, int sockfd){
    
    
    char men_log[10000]="";
    /* Inicializacion de la cabecera de los mensajes de log */
    strcat (men_log,"::DEV::");
    strcat(men_log, getenv("PWD"));
    strcat(men_log, "::-");
    
    int bucle=1, n;
    char buffer[10000], transfer[100000];
    close(sockfd);
    while (bucle==1) {
        memset(buffer, 0, 10000);
        //bzero(buffer,10000);
        /* Leyendo de la conexion */
        n = read(newsockfd,buffer,9999);
        if (n < 0){
            openlog (men_log, 0, LOG_LOCAL4);
            syslog(LOG_ERR, "Error al leer del socket\n Archivo:funciones.h \nFuncion: mandar_logs");
            closelog();
            exit(1);
        }
        strcat(transfer, buffer);
		if (strstr(transfer,"</frm>")!=NULL) {

//        if ((transfer[strlen(transfer)-1]) == '\n'){
            /* Mandando a syslog */
			//printf("%s\n", transfer);
            openlog (men_log, 0, LOG_LOCAL5);
            syslog (LOG_INFO, "%s",transfer);
            closelog();
            memset(transfer, 0, 100000);
            //bzero(transfer,100000);
        }
    }
}


//Funcion que implementa el servidor de RabbitMQ para la gestión de mensajes
void gestion_mensajes2(int newsockfd, char* mac){
    
	char const *hostname; //Nombre del host
	int port, status;
	char const *exchange; //Metodo de envío
	char const *bindingkey; //Clave de intercambio
	amqp_socket_t *socket = NULL;
	amqp_connection_state_t conn;
	
	amqp_bytes_t queuename;
	
	hostname = "localhost";
	port = 5672;
	exchange = "amq.direct"; 
	bindingkey = mac; //La MAC del dispositivo de VOLTA
	
	//Establecimiento de la conexion con el servidor
	conn = amqp_new_connection();
	
	socket = amqp_tcp_socket_new(conn);
	if (!socket) {
		die("creating TCP socket");
	}
	
	status = amqp_socket_open(socket, hostname, port);
	if (status) {
		die("opening TCP socket");
	}
	
	//Login
	die_on_amqp_error(amqp_login(conn, "/", 0, 131072, 0, AMQP_SASL_METHOD_PLAIN, "guest", "guest"),
					  "Logging in");
	amqp_channel_open(conn, 1);
	die_on_amqp_error(amqp_get_rpc_reply(conn), "Opening channel");
	
	//Declaracion de la cola de mensajes
	{
		amqp_queue_declare_ok_t *r = amqp_queue_declare(conn, 1, amqp_empty_bytes, 0, 0, 0, 1,
														amqp_empty_table);
		die_on_amqp_error(amqp_get_rpc_reply(conn), "Declaring queue");
		queuename = amqp_bytes_malloc_dup(r->queue);
		if (queuename.bytes == NULL) {
			fprintf(stderr, "Out of memory while copying queue name");
			exit(1);
		}
	}
	
	amqp_queue_bind(conn, 1, queuename, amqp_cstring_bytes(exchange), amqp_cstring_bytes(bindingkey),
					amqp_empty_table);
	die_on_amqp_error(amqp_get_rpc_reply(conn), "Binding queue");
	
	amqp_basic_consume(conn, 1, queuename, amqp_empty_bytes, 0, 1, 0, amqp_empty_table);
	die_on_amqp_error(amqp_get_rpc_reply(conn), "Consuming");
	
	//Ejecutando metodo run, que gestiona los mensajes
	run(conn,newsockfd, mac);
	
	die_on_amqp_error(amqp_channel_close(conn, 1, AMQP_REPLY_SUCCESS), "Closing channel");
	die_on_amqp_error(amqp_connection_close(conn, AMQP_REPLY_SUCCESS), "Closing connection");
	die_on_error(amqp_destroy_connection(conn), "Ending connection");
	
}


void run(amqp_connection_state_t conn,int newsockfd, char* mac)
{
	uint64_t start_time = now_microseconds();
	int received = 0;
	int previous_received = 0;
	uint64_t previous_report_time = start_time;
	uint64_t next_summary_time = start_time + SUMMARY_EVERY_US;
	
	amqp_frame_t frame;
	
	uint64_t now;
	char men_log[10000]="";
    /* Inicializacion de la cabecera de los mensajes de log */
    strcat (men_log,"::DEV::");
    strcat(men_log, getenv("PWD"));
    strcat(men_log, "::-");
	{
		amqp_rpc_reply_t ret;
		amqp_envelope_t envelope;
		
		now = now_microseconds();
		if (now > next_summary_time) {
			int countOverInterval = received - previous_received;
			double intervalRate = countOverInterval / ((now - previous_report_time) / 1000000.0);
			//printf("%d ms: Received %d - %d since last report (%d Hz)\n",
				   (int)(now - start_time) / 1000, received, countOverInterval, (int) intervalRate;
			
			previous_received = received;
			previous_report_time = now;
			next_summary_time += SUMMARY_EVERY_US;
		}
		
		amqp_maybe_release_buffers(conn);
		ret = amqp_consume_message(conn, &envelope, NULL, 0);
		
		if (AMQP_RESPONSE_NORMAL != ret.reply_type) {
			if (AMQP_RESPONSE_LIBRARY_EXCEPTION == ret.reply_type &&
				AMQP_STATUS_UNEXPECTED_STATE == ret.library_error) {
				if (AMQP_STATUS_OK != amqp_simple_wait_frame(conn, &frame)) {
					//break;
				}
				
				if (AMQP_FRAME_METHOD == frame.frame_type) {
					switch (frame.payload.method.id) {
						case AMQP_BASIC_ACK_METHOD:
							/* if we've turned publisher confirms on, and we've published a message
							 * here is a message being confirmed
							 */
							
							break;
						case AMQP_BASIC_RETURN_METHOD:
							/* if a published message couldn't be routed and the mandatory flag was set
							 * this is what would be returned. The message then needs to be read.
							 */
						{
							amqp_message_t message;
							ret = amqp_read_message(conn, frame.channel, &message, 0);
							if (AMQP_RESPONSE_NORMAL != ret.reply_type) {
								break;
							}
							
							amqp_destroy_message(&message);
						}
							
							break;
							
						case AMQP_CHANNEL_CLOSE_METHOD:
							/* a channel.close method happens when a channel exception occurs, this
							 * can happen by publishing to an exchange that doesn't exist for example
							 *
							 * In this case you would need to open another channel redeclare any queues
							 * that were declared auto-delete, and restart any consumers that were attached
							 * to the previous channel
							 */
							break;
							
						case AMQP_CONNECTION_CLOSE_METHOD:
							/* a connection.close method happens when a connection exception occurs,
							 * this can happen by trying to use a channel that isn't open for example.
							 *
							 * In this case the whole connection must be restarted.
							 */
							break;
							
						default:
							fprintf(stderr ,"An unexpected method was received %d\n", frame.payload.method.id);
							break;
					}
				}
			}
			
		} else {
			if (envelope.message.properties._flags & AMQP_BASIC_CONTENT_TYPE_FLAG) {
				//Comprobamos el formato del mensaje recibido
				if (json_parser((char *) envelope.message.body.bytes,mac)==NULL) {
					openlog (men_log, 0, LOG_LOCAL4);
					syslog(LOG_ERR, "Error en el mensaje. Por favor compruebe el formato");
					closelog();
					
				}else{
					//Enviamos la orden al gateway
					int n = write(newsockfd,json_parser((char *) envelope.message.body.bytes,mac),
								  strlen(json_parser((char *) envelope.message.body.bytes,mac)));
					if (n < 0)
					{
						openlog (men_log, 0, LOG_LOCAL4);
						syslog(LOG_ERR, "Error al enviar el mensaje\n Archivo:funciones.h \nFuncion: run \n");
						closelog();
					
					}else{
						//Comprobamos que la orden se ha ejecutado con las funciones comprobar_encendido y comprobar_apagado
						//Encendido
						if (strstr(json_parser((char *) envelope.message.body.bytes,mac),"on")!=NULL) {
							if (strstr(json_parser((char *) envelope.message.body.bytes,mac),"001")!=NULL){
								comprobar_encendido(mac, 1, newsockfd, conn);
							}else if (strstr(json_parser((char *) envelope.message.body.bytes,mac),"002")!=NULL){
								comprobar_encendido(mac, 2, newsockfd, conn);
							}else if (strstr(json_parser((char *) envelope.message.body.bytes,mac),"003")!=NULL){
								comprobar_encendido(mac, 3, newsockfd, conn);
							}else if (strstr(json_parser((char *) envelope.message.body.bytes,mac),"004")!=NULL){
								comprobar_encendido(mac, 4, newsockfd, conn);
							}else if (strstr(json_parser((char *) envelope.message.body.bytes,mac),"005")!=NULL){
								comprobar_encendido(mac, 5, newsockfd, conn);
							}else if (strstr(json_parser((char *) envelope.message.body.bytes,mac),"006")!=NULL){
								comprobar_encendido(mac, 6, newsockfd, conn);
							}else if (strstr(json_parser((char *) envelope.message.body.bytes,mac),"007")!=NULL){
								comprobar_encendido(mac, 7, newsockfd, conn);
							}else if (strstr(json_parser((char *) envelope.message.body.bytes,mac),"008")!=NULL){
								comprobar_encendido(mac, 8, newsockfd, conn);
							}else if (strstr(json_parser((char *) envelope.message.body.bytes,mac),"009")!=NULL){
								comprobar_encendido(mac, 9, newsockfd, conn);
							}else if (strstr(json_parser((char *) envelope.message.body.bytes,mac),"010")!=NULL){
								comprobar_encendido(mac, 10, newsockfd, conn);
							}
						//Apagado
						}else if (strstr(json_parser((char *) envelope.message.body.bytes,mac),"off")!=NULL){
							if (strstr(json_parser((char *) envelope.message.body.bytes,mac),"001")!=NULL){
								comprobar_apagado(mac, 1, newsockfd, conn);
							}else if (strstr(json_parser((char *) envelope.message.body.bytes,mac),"002")!=NULL){
								comprobar_apagado(mac, 2, newsockfd, conn);
							}else if (strstr(json_parser((char *) envelope.message.body.bytes,mac),"003")!=NULL){
								comprobar_apagado(mac, 3, newsockfd, conn);
							}else if (strstr(json_parser((char *) envelope.message.body.bytes,mac),"004")!=NULL){
								comprobar_apagado(mac, 4, newsockfd, conn);
							}else if (strstr(json_parser((char *) envelope.message.body.bytes,mac),"005")!=NULL){
								comprobar_apagado(mac, 5, newsockfd, conn);
							}else if (strstr(json_parser((char *) envelope.message.body.bytes,mac),"006")!=NULL){
								comprobar_apagado(mac, 6, newsockfd, conn);
							}else if (strstr(json_parser((char *) envelope.message.body.bytes,mac),"007")!=NULL){
								comprobar_apagado(mac, 7, newsockfd, conn);
							}else if (strstr(json_parser((char *) envelope.message.body.bytes,mac),"008")!=NULL){
								comprobar_apagado(mac, 8, newsockfd, conn);
							}else if (strstr(json_parser((char *) envelope.message.body.bytes,mac),"009")!=NULL){
								comprobar_apagado(mac, 9, newsockfd, conn);
							}else if (strstr(json_parser((char *) envelope.message.body.bytes,mac),"010")!=NULL){
								comprobar_apagado(mac, 10, newsockfd, conn);
							}
						}
					}
				}
			}
			//Destruimos el mensaje
			amqp_destroy_envelope(&envelope);
		}
		received++;
		gestion_mensajes2(newsockfd,mac);
	}
}

//Parser del JSON recibido, que averigua la orden enviada por el usuario
char *json_parser (char* mensaje, char* mac){
	
	char pos[4];
	char* query1_enc="<frm><cmd>#stt:on@";
	char* query2_enc="</cmd></frm>";
	char* query1_ap="<frm><cmd>#stt:off@";
	char* query2_ap="</cmd></frm>";
	char* query1_enl="<frm><cmd>#add:@";
	char* query2_enl="</cmd></frm>";
	char* orden=NULL;
	int r;
    jsmn_parser parser;
    jsmntok_t tokens[256];
	
    //Inicialización del parser
    jsmn_init(&parser);
    const char *js= mensaje;
	//Se parsea el mensaje
	r = jsmn_parse(&parser, js, strlen(js), tokens, 256);
	//Si corresponde a la mac del hilo
	if (comparar_cadena_token(js, tokens[4], mac)==1) {
		for (int i = tokens[6].start; i<tokens[6].end ; i++) {
			//printf("%c", js[i]);
			pos[i-tokens[6].start]=js[i];
		}
		//Si es la orden de apagado
		if (comparar_cadena_token(js, tokens[2], "apagar")){
			orden= malloc(strlen(query1_ap)+strlen(query2_ap)+4);
			strcat(orden,query1_ap);
			strcat(orden,pos);
			strcat(orden,query2_ap);
		//Si es la orden de encendido
		}else if(comparar_cadena_token(js, tokens[2], "encender")){
			orden= malloc(strlen(query1_enc)+strlen(query2_enc)+4);
			strcat(orden,query1_enc);
			strcat(orden,pos);
			strcat(orden,query2_enc);
		//Si es la orden de enlace
		}else if(comparar_cadena_token(js, tokens[2], "enlazar")){
			orden= malloc(strlen(query1_enl)+strlen(query2_enl)+4);
			strcat(orden,query1_enl);
			strcat(orden,pos);
			strcat(orden,query2_enl);
		}
	}
	return orden;
}

/* Esta funcion comprueba que el enchufe se enciende, enviando un mensaje de comprobacion al gateway y a partir de la respuesta envia el mensaje correspondiente 
por RabbitMQ con la funcion mandar_respuesta */
void comprobar_encendido(char* mac, int pos, int newsockfd, amqp_connection_state_t conn){
	
	int resultado=3;
	char men_log[10000]="";
	char pos_s[3];
	char* query1="<frm><req>#stt:@";	//Primera parte de la orden
	char* query2="</req></frm>";	//Segunda parte de la orden
	char* query=malloc(strlen(query1)+strlen(query2)+4);
	char* res1="<state_";	//Primera parte de la referencia
	char* res2=">on</state_";	//Segunda parte de la referencia
	char* respuesta=malloc(strlen(res1)+strlen(res2)+4);
    /* Inicializacion de la cabecera de los mensajes de log */
    strcat (men_log,"::DEV::");
    strcat(men_log, getenv("PWD"));
    strcat(men_log, "::-");
	
	char buffer[10000], transfer[100000];
	int n;
	/* Inicializamos mensaje de comprobacion, en funcion de la posicion, se concatena junto con las 
	 dos partes y se da origen a la orden final */
	if (pos!=10) {
		sprintf(pos_s, "%d", pos);
		strcat(query,query1);
		strcat(query,"00");
		strcat(query,pos_s);
		strcat(query,query2);
	}else{
		sprintf(pos_s, "%d", pos);
		strcat(query,query1);
		strcat(query,"0");
		strcat(query,pos_s);
		strcat(query,query2);
	}
	/* Con pos_s, lo concatenamos para poder tener la referencia a comparar*/
	strcat(respuesta,res1);
    strcat(respuesta,pos_s);
    strcat(respuesta,res2);
	strcat(respuesta,pos_s);
    strcat(respuesta,">");
	
	/* Escribimos la comprobación al Gateway */
	n = write(newsockfd,query,strlen(query));
	if (n < 0){
		openlog (men_log, 0, LOG_LOCAL4);
		syslog(LOG_ERR, "Error al escribir el mensaje de comprobacion al Gateway\n Archivo:funciones.h \nFuncion: comprobar_encendido");
		closelog();
		exit(1);
	}
	
	while (resultado==3) {
        memset(buffer, 0, 10000);
        //bzero(buffer,10000);
		//Leemos del socket para saber el estado del enchufe
        n = read(newsockfd,buffer,9999);
        if (n < 0){
            openlog (men_log, 0, LOG_LOCAL4);
            syslog(LOG_ERR, "Error al leer del socket\n Archivo:funciones.h \nFuncion: comprobar_encendido");
            closelog();
            exit(1);
        }
        strcat(transfer, buffer);
		if (strstr(transfer,"</frm>")!=NULL) {

        //if ((transfer[strlen(transfer)-1]) == '\n'){
			
			openlog (men_log, 0, LOG_LOCAL5);
			syslog (LOG_INFO, "%s",transfer);
			closelog();
			//Si no es un mensaje de espera
			if (strstr(transfer,mac)!=NULL || strstr(transfer,"<rpt>")!=NULL) {
				/*Analizamos el mensaje, comparando el valor que tiene el enchufe en el mensaje con el que debería ser, en funcion de la posicion
				que ocupa el interruptor en la lista del Gateway
				Devuelve a RabbitMQ 1 si todo fue bien y 0 en caso de error, a traves de la funcion */
				if (strstr(transfer, "Socket on")!=NULL || strstr(transfer, respuesta)!=NULL) {
					resultado=1;
					mandar_respuesta("localhost", "5672", "amq.direct", "encendido", "1", conn);
				}else {
					resultado=0;
					mandar_respuesta("localhost", "5672", "amq.direct", "encendido", "0", conn);
				}
			}
			memset(transfer, 0, 100000);
            //bzero(transfer,100000);		
		}
    }
}

/* Esta funcion comprueba que el enchufe se apaga, enviando un mensaje de comprobacion al gateway y a partir de la respuesta envia el mensaje correspondiente 
 por RabbitMQ con la funcion mandar_respuesta */
void comprobar_apagado(char* mac, int pos, int newsockfd, amqp_connection_state_t conn){
	
	int resultado=3;
	char men_log[10000]="";
	char pos_s[3];
	char* query1="<frm><req>#stt:@";	//Primera parte de la orden
	char* query2="</req></frm>";	//Segunda parte de la orden
	char* query=malloc(strlen(query1)+strlen(query2)+4);
	char* res1="<state_";	//Primera parte de la referencia
	char* res2=">off</state_";	//Segunda parte de la referencia
	char* respuesta=malloc(strlen(res1)+strlen(res2)+4);
	
    /* Inicializacion de la cabecera de los mensajes de log */
    strcat (men_log,"::DEV::");
    strcat(men_log, getenv("PWD"));
    strcat(men_log, "::-");
	
	char buffer[10000], transfer[100000];
	int n;
	/* Inicializamos mensaje de comprobacion, en funcion de la posicion, se concatena junto con las 
	 dos partes y se da origen a la orden final */	
	if (pos!=10) {
		sprintf(pos_s, "%d", pos);
		strcat(query,query1);
		strcat(query,"00");
		strcat(query,pos_s);
		strcat(query,query2);
	}else {
		sprintf(pos_s, "%d", pos);
		strcat(query,query1);
		strcat(query,"0");
		strcat(query,pos_s);
		strcat(query,query2);
	}
	/* Con pos_s, lo concatenamos para poder tener la referencia a comparar*/
	strcat(respuesta,res1);
    strcat(respuesta,pos_s);
    strcat(respuesta,res2);
	strcat(respuesta,pos_s);
    strcat(respuesta,">");

	
	
	/* Escribimos la comprobación al Gateway */
	n = write(newsockfd,query,strlen(query));
	if (n < 0){
		openlog (men_log, 0, LOG_LOCAL4);
		syslog(LOG_ERR, "Error al escribir el mensaje de comprobacion al Gateway\n Archivo:funciones.h \nFuncion: comprobar_apagado");
		closelog();
		exit(1);
	}
	
	while (resultado==3) {
		memset(buffer, 0, 10000);
        //bzero(buffer,10000);		//Leemos del socket para saber el estado del enchufe
        n = read(newsockfd,buffer,9999);
        if (n < 0){
            openlog (men_log, 0, LOG_LOCAL4);
            syslog(LOG_ERR, "Error al leer del socket\n Archivo:funciones.h \nFuncion: comprobar_apagado");
            closelog();
            exit(1);
        }
        strcat(transfer, buffer);
		if (strstr(transfer,"</frm>")!=NULL) {

        //if ((transfer[strlen(transfer)-1]) == '\n'){
			
			openlog (men_log, 0, LOG_LOCAL5);
			syslog (LOG_INFO, "%s",transfer);
			closelog();
			if (strstr(transfer,mac)!=NULL || strstr(transfer,"<rpt>")!=NULL) {	//Si no es un mensaje de espera
				/*Analizamos el mensaje, comparando el valor que tiene el enchufe en el mensaje con el que debería ser, en funcion de la posicion
				 que ocupa el interruptor en la lista del Gateway
				 Devuelve a RabbitMQ 1 si todo fue bien y 0 en caso de error, a traves de la funcion */
				if (strstr(transfer, "Socket off")!=NULL || strstr(transfer, respuesta)!=NULL) {
					resultado=1;
					mandar_respuesta("localhost", "5672", "amq.direct", "apagado", "1", conn);
				}else {
					resultado=0;
					mandar_respuesta("localhost", "5672", "amq.direct", "apagado", "0", conn);
				}
			}
			memset(transfer, 0, 100000);
            //bzero(transfer,100000);		
		}
	}
}

/* Esta funcion envía a RabbitMQ el mensaje con la comprobacion final de que se ha ejecutado o no la orden */
void mandar_respuesta(char* hostname, char* port, char* exchange, char* routingkey, char* messagebody, amqp_connection_state_t conn){

		amqp_basic_properties_t props;
		props._flags = AMQP_BASIC_CONTENT_TYPE_FLAG | AMQP_BASIC_DELIVERY_MODE_FLAG;
		props.content_type = amqp_cstring_bytes("text/plain");
		props.delivery_mode = 2; /* persistent delivery mode */
		die_on_error(amqp_basic_publish(conn,
										1,
										amqp_cstring_bytes(exchange),
										amqp_cstring_bytes(routingkey),
										0,
										0,
										&props,
										amqp_cstring_bytes(messagebody)),
					 "Publishing");
	
	die_on_amqp_error(amqp_channel_close(conn, 1, AMQP_REPLY_SUCCESS), "Closing channel");
	die_on_amqp_error(amqp_connection_close(conn, AMQP_REPLY_SUCCESS), "Closing connection");
	die_on_error(amqp_destroy_connection(conn), "Ending connection");
}
