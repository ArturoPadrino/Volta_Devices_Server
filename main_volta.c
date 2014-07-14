/* Programa principal del servidor de dispositivos de VOLTA */


#include "syslog.h"
#include "sys/socket.h"
#include "sys/un.h"
#include "sys/types.h"
#include "sys/socket.h"
#include "netinet/in.h"
#include "sys/stat.h"
#include "fcntl.h"
#include "stdlib.h"
#include "string.h"
#include "unistd.h"
#include "errno.h"
#include "stdio.h"
#include "string.h"



//Archivo de cabecera con las funciones
#include "funciones.h"

//Numero de puerto
#define PORTNO 5005


int main( int argc, char *argv[] ){

    printf("HOLA MUNDO");    
    char men_log[10000]="";
    int sockfd, newsockfd, clilen;
    struct sockaddr_in serv_addr, cli_addr;
    int bucle=1;
    
    /* Inicializacion de la cabecera de los mensajes de log */
    strcat (men_log,"::DEV::");
    strcat(men_log, getenv("PWD"));
    strcat(men_log, "::-");
    
    
    /* Llamada a la funcion socket() */

    sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd < 0){
        openlog (men_log, 0, LOG_LOCAL4);
        syslog(LOG_ERR, "Error al abrir el socket de la conexión \nArchivo:main_volta.c");
        closelog();
        exit(1);    
	}
    
    /* Inicializacion del socket */
	//bzero((char *) &serv_addr, sizeof(serv_addr));
    memset((char *) &serv_addr, 0, sizeof(serv_addr));    
	serv_addr.sin_family = AF_INET;
    serv_addr.sin_addr.s_addr = INADDR_ANY;
    serv_addr.sin_port = htons(PORTNO);
    
    /* Unimos el socket a la direccion de nuestro host con la llamada a bind() */
    if (bind(sockfd, (struct sockaddr *) &serv_addr, sizeof(serv_addr)) < 0){
        openlog (men_log, 0, LOG_LOCAL4);
        syslog(LOG_ERR, "Error en la llamada a bind() \nArchivo:main_volta.c");
        closelog();
        exit(1);
    }
    

    while (bucle == 1)
    {
        /* Comenzamos a escuchar a los mensajes entrantes, aquí el proceso entrara
         * en modo de espera, hasta que aparezcan nuevas conexiones*/
        listen(sockfd,5);
        clilen = sizeof(cli_addr);
		char mac[12];
        newsockfd = accept(sockfd, (struct sockaddr *) &cli_addr, &clilen);
        
        if (newsockfd < 0)
        {
            openlog (men_log, 0, LOG_LOCAL4);
            syslog(LOG_ERR, "Error en la función accept() \nArchivo:main_volta.c");
            closelog();
            exit(1);
        }
		//Hallamos la MAC del gateway que se acaba de conectar
		hallar_mac(newsockfd, mac);
        // Creación del proceso hijo
        int pid = fork();
        if (pid < 0)
        {
            openlog (men_log, 0, LOG_LOCAL4);
            syslog(LOG_ERR, "Error en la función fork() \nArchivo:main_volta.c");
            closelog();
            exit(1);
        }
        if (pid == 0)
        {
            // Proceso hijo
            mandar_logs(newsockfd,sockfd);
        }
        else
        {
            // Creación del segundo proceso hijo
            int pid2 = fork();

            if (pid2 < 0)
            {
                openlog (men_log, 0, LOG_LOCAL4);
                syslog(LOG_ERR, "Error en la función fork() 2 \nArchivo:main_volta.c");
                closelog();
                exit(1);
            }
            if (pid2 == 0)
            {
                //Segundo proceso hijo
                gestion_mensajes2(newsockfd, mac);
            }
                
            close(newsockfd);
        }
    }
    return 0;
}
