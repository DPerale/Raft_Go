package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"net/http"

	//"net"
	"math/rand"
	"net/url"

	//"strings"
	"math"
	"strconv"
	"sync"

	// "sync"
	"bufio"
	"os"
	"time"
)

type log_atom struct {
	term  int
	value int
}

var server_log []log_atom
var server_log_temp []log_atom
var number_commit_values int

type state struct {
	server_num int

	state     int
	term      int
	max_index int

	hb_received bool

	term_vote_send      int
	term_votes_received int
}

var m_state sync.Mutex
var m_server_log sync.Mutex
var m_server_log_temp sync.Mutex

// funzione che permette il recupero senza uscire del tutto dall'applicazione
// in caso di errore
func recovery() {
	if r := recover(); r != nil {
		fmt.Println("recovered:", r)
	}
}

type MoreParameters struct {
	server_addresses *[]string
	server_state     *state
}

func commit_message(server_addresses *[]string, server_address string, server_state *state, term int, value int) {
	defer recovery()
	client := http.Client{Timeout: 300 * time.Millisecond}
	resp, err := client.Get("http://" + server_address + "/commit?term=" + strconv.Itoa(term) + "&value" + strconv.Itoa(value))
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	index_received, _ := strconv.Atoi(string(body))
	m_state.Lock()
	if index_received != (server_state.max_index + 1) {
		m_state.Unlock()
	} else {
		m_state.Unlock()
		fmt.Println("/ricevuta risposta commit")

		m_server_log_temp.Lock()
		number_commit_values = number_commit_values + 1
		half_servers := math.Floor(float64(server_state.server_num)/2) + 1
		if number_commit_values == int(half_servers) {
			m_server_log.Lock()
			m_state.Lock()
			server_log = append(server_log, server_log_temp[0])
			server_state.max_index = server_state.max_index + 1
			m_state.Unlock()
			m_server_log.Unlock()
			server_log_temp = server_log_temp[1:]
			number_commit_values = 0
			if len(server_log_temp) <= 0 {
				m_server_log_temp.Unlock()
			} else {
				value := server_log_temp[0].value
				number_commit_values = number_commit_values + 1
				m_server_log_temp.Unlock()
				for i := 0; i < len(*server_addresses); i++ {
					go append_entries_message(server_addresses, (*server_addresses)[i], server_state, term, value)
				}
			}

		}
	}
}

func append_entries_message(server_addresses *[]string, server_address string, server_state *state, term int, value int) {
	defer recovery()
	client := http.Client{Timeout: 300 * time.Millisecond}
	resp, err := client.Get("http://" + server_address + "/append_entries?term=" + strconv.Itoa(term) + "&value" + strconv.Itoa(value))
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	index_received, _ := strconv.Atoi(string(body))
	m_state.Lock()
	if index_received != (server_state.max_index + 1) {
		m_state.Unlock()
	} else {
		m_state.Unlock()
		fmt.Println("/ricevuta risposta appendentries")

		m_server_log_temp.Lock()
		number_commit_values = number_commit_values + 1
		half_servers := math.Floor(float64(server_state.server_num)/2) + 1
		if number_commit_values == int(half_servers) {
			number_commit_values = 0
			m_server_log_temp.Unlock()
			for i := 0; i < len(*server_addresses); i++ {
				go commit_message(server_addresses, (*server_addresses)[i], server_state, term, value)
			}
		}
	}
}

func (mp *MoreParameters) handler(w http.ResponseWriter, r *http.Request) {
	//time.Sleep(1 * time.Second)

	//fmt.Println(mp.server_state.term)

	type_of_message := r.URL.Path
	parameters, _ := url.ParseQuery(r.URL.RawQuery)

	switch type_of_message {
	case "/request_vote":
		term_received, _ := strconv.Atoi(parameters["term"][0])
		log_term_received, _ := strconv.Atoi(parameters["log_term"][0])
		index_received, _ := strconv.Atoi(parameters["index"][0])
		up_to_date := false
		m_state.Lock()
		m_server_log.Lock()
		if server_log[mp.server_state.max_index].term < log_term_received {
			up_to_date = true
		} else {
			if server_log[mp.server_state.max_index].term == log_term_received && index_received >= mp.server_state.max_index {
				up_to_date = true
			}
		}
		m_server_log.Unlock()
		if term_received < mp.server_state.term || term_received == mp.server_state.term_vote_send || !up_to_date {
			m_state.Unlock()
		} else {
			mp.server_state.term_vote_send = term_received
			m_state.Unlock()
			fmt.Println("/request_vote")
			w.Write([]byte(parameters["term"][0]))
		}
	case "/heartbeat":
		m_state.Lock()
		term_received, _ := strconv.Atoi(parameters["term"][0])
		if term_received < mp.server_state.term {
			m_state.Unlock()
		} else {
			mp.server_state.hb_received = true
			mp.server_state.state = 1
			mp.server_state.term = term_received
			m_state.Unlock()
			w.Write([]byte(parameters["term"][0]))
		}
	case "/wakeup":
		m_state.Lock()
		mp.server_state.state = 1
		m_state.Unlock()
		fmt.Println("/wakeup")
	case "/newinput":
		fmt.Println("/newinput")
		m_state.Lock()
		term := mp.server_state.term
		if mp.server_state.state == 0 {
			m_state.Unlock()
			value_received, _ := strconv.Atoi(parameters["value"][0])
			m_server_log_temp.Lock()
			server_log_temp = append(server_log_temp, log_atom{term, value_received})
			number_commit_values = number_commit_values + 1
			m_server_log_temp.Unlock()
			for i := 0; i < len(*mp.server_addresses); i++ {
				go append_entries_message(mp.server_addresses, (*mp.server_addresses)[i], mp.server_state, term, value_received)
			}
		} else {
			m_state.Unlock()
		}
	case "/append_entries":
		fmt.Println("/append_entries")
	default:
		fmt.Println("cosa vuoi")
	}

	//w.Write([]byte(type_of_message))
}

func request_vote_message(server_address string, server_state *state, term int, log_term int, index int) {
	defer recovery()
	fmt.Println(term)
	client := http.Client{Timeout: 300 * time.Millisecond}
	resp, err := client.Get("http://" + server_address + "/request_vote?term=" + strconv.Itoa(term) + "&log_term=" + strconv.Itoa(log_term) + "&index=" + strconv.Itoa(index))
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	term_received, _ := strconv.Atoi(string(body))
	m_state.Lock()
	if term_received != server_state.term {
		m_state.Unlock()
	} else {
		server_state.term_votes_received = server_state.term_votes_received + 1
		half_servers := math.Floor(float64(server_state.server_num)/2) + 1
		if server_state.term_votes_received == int(half_servers) && server_state.state == 2 {
			server_state.state = 0
		}
		m_state.Unlock()
	}
}

func request_vote(server_addresses *[]string, server_state *state) {
	for true {
		m_state.Lock()
		if server_state.hb_received || server_state.state == 0 || server_state.state == 3 {
			server_state.hb_received = false
			m_state.Unlock()
		} else {
			server_state.term = server_state.term + 1
			server_state.state = 2
			server_state.term_votes_received = 1
			term := server_state.term
			m_state.Unlock()
			for i := 0; i < len(*server_addresses); i++ {
				go request_vote_message((*server_addresses)[i], server_state, term)
			}
		}
		time.Sleep(time.Duration(rand.Intn(400-300)+300) * time.Millisecond)
	}
}

func heartbeat_message(server_address string, term int) {
	defer recovery()
	client := http.Client{Timeout: 300 * time.Millisecond}
	resp, err := client.Get("http://" + server_address + "/heartbeat?term=" + strconv.Itoa(term))
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()
}

func heartbeat(server_addresses *[]string, server_state *state) {
	for true {
		m_state.Lock()
		if server_state.state == 0 {
			term := server_state.term
			m_state.Unlock()
			for i := 0; i < len(*server_addresses); i++ {
				go heartbeat_message((*server_addresses)[i], term)
			}
		} else {
			m_state.Unlock()
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func main() {
	// lettura indirizzi altri server
	file, err := os.Open("addresses.txt")
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	num_server := 1
	var server_addresses []string
	for scanner.Scan() {
		server_addresses = append(server_addresses, scanner.Text())
		num_server = num_server + 1
	}
	for i := 0; i < num_server-1; i++ {
		fmt.Println(server_addresses[i])
	}

	//inizializzazione parametri server
	server_state := state{
		server_num:          num_server,
		state:               3,
		term:                0,
		max_index:           0,
		hb_received:         false,
		term_vote_send:      0,
		term_votes_received: 0}

	//task periodici
	go request_vote(&server_addresses, &server_state)
	go heartbeat(&server_addresses, &server_state)

	// inizializzazione server http
	more_parameters := &MoreParameters{&server_addresses, &server_state}

	http.HandleFunc("/", more_parameters.handler)
	if err := http.ListenAndServe(":80", nil); err != nil {
		fmt.Println(err)
	}
}

// func multiple_message() {
// 	for i := 0; i < 10; i++ {
// 		go message(i)
// 		go message(i)
// 		time.Sleep(1 * time.Second)
// 	}

// }

// func message(i int) {
// 	fmt.Println(i)
// 	if i == 0 {
// 		defer recovery()
// 		resp, err := http.Get("http://192.168.1.169/prova")
// 		if err != nil {
// 			panic("ciao")
// 		}
// 		if resp.StatusCode != 200 {
// 			fmt.Println("200")
// 			b, _ := ioutil.ReadAll(resp.Body)
// 			log.Fatal(string(b))
// 		}
// 		defer resp.Body.Close()
// 	} else {
// 		defer recovery()
// 		resp, err := http.Get("http://192.168.1.168/prova")
// 		if err != nil {
// 			fmt.Println("errore")
// 		}
// 		defer resp.Body.Close()
// 	}
// }
