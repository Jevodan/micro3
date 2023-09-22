package main

import (
	"fmt"
	"io"
	"log"
	"net/http"

	"github.com/streadway/amqp"
)

/*
Давай такое.
1ый сервис:
Два метода.
1 - получить пост запросом урл, положить этот урл в кролик. ---------postHandler------------- Postman: {"url":"http://vk.com","url":"http://yandex.ru"}
2 - гет метод. получить список доступных урлов.				---------getUrlsHandler----------
*/

const (
	QUEUE_NAME string = "first_queue"
)

var (
	ch *amqp.Channel
)

func main() {
	http.HandleFunc("/info", getUrlsHandler)
	http.HandleFunc("/add", postHandler)
	ch = rabbitInit()
	queueInit(ch)

	http.ListenAndServe(":8080", nil)
}

// добавляет сообщение в очередь  localhost://8000/add
func postHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Метод не разрешен", http.StatusMethodNotAllowed)
		return
	}
	body, err1 := io.ReadAll(r.Body)
	if err1 != nil {
		http.Error(w, "Ошибка при чтении тела запроса", http.StatusBadRequest)
		return
	}
	// формат сообщения, опции (передаем в байтах)
	message := amqp.Publishing{
		ContentType: "text/plain",
		Body:        body,
	}

	// Отправляем сообщение в очередь

	err := ch.Publish(
		"",         // Обменник (exchange) - пустая строка для обмена по умолчанию
		QUEUE_NAME, // Имя очереди
		false,      // Mandatory
		false,      // Immediate
		message,
	)
	failOnError(err, "Не удалось опубликовать сообщение")
	fmt.Printf("Отправлено: %s\n", string(body))
}

// Выводит информацию об очереди  localhost://8000/info
func getUrlsHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Метод не разрешен", http.StatusMethodNotAllowed)
		return
	}
	msgs, err := ch.Consume(
		QUEUE_NAME, // Имя очереди
		"",         // Consumer
		true,       // AutoAck - автоматическое подтверждение получения сообщения
		false,      // Exclusive
		false,      // NoLocal
		false,      // NoWait
		nil,        // Args
	)
	failOnError(err, "Не удалось зарегистрировать потребителя")
	for d := range msgs {
		fmt.Printf("Получено сообщение: %s\n", d.Body)
	}
	defer ch.Close()

	fmt.Println("Ожидание сообщений. Для завершения нажмите CTRL+C")
	select {}
}

// Инициализация очереди кролика
func queueInit(ch *amqp.Channel) {
	_, err := ch.QueueDeclare(
		QUEUE_NAME, // Имя очереди
		true,       // Долговечность очереди
		false,      // Удаление очереди, когда нет активных подписчиков
		false,      // Эксклюзивность очереди
		false,      // Не ждать подтверждение доставки
		nil,        // Дополнительные аргументы
	)
	failOnError(err, "Ошибка при объявлении очереди: %v")
}

// Подключаемся к RabbitMQ
func rabbitInit() *amqp.Channel {
	// Строка для подключения к RabbitMQ, обычно имеет форму "amqp://имя_пользователя:пароль@хост:порт/"
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	failOnError(err, "Не удалось подключиться к RabbitMQ")
	//defer conn.Close()

	// канал (channel), через который будете взаимодействовать с RabbitMQ:
	ch, err := conn.Channel()
	failOnError(err, "Не удалось открыть канал")
	//	defer ch.Close()
	return ch
}

// Паттерн обработки ошибок
func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s : %s", msg, err)
	}
}
