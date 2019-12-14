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
	term   int
	value  int
	client string
}

type state struct {
	id         string
	id_leader  string
	server_num int

	state     int
	term      int
	max_index int

	hb_received bool

	term_vote_send      int
	term_votes_received int
}

var server_log []log_atom
var server_map map[string][]int
var m_state sync.Mutex
var m_server_log sync.Mutex
var server_state state
var server_addresses []string

// funzione che permette il recupero senza uscire del tutto dall'applicazione
// in caso di errore
func recovery() {
	if r := recover(); r != nil {
		fmt.Println("recovered:", r)
	}
}

// func append_entries_message(server_addresses *[]string, server_address string, server_state *state, term int, value int, log_term int, index int) {
// 	defer recovery()
// 	client := http.Client{Timeout: 300 * time.Millisecond}
// 	resp, err := client.Get("http://" + server_address + "/append_entries?term=" + strconv.Itoa(term) + "&value" + strconv.Itoa(value) + "&log_term=" + strconv.Itoa(log_term) + "&index=" + strconv.Itoa(index))
// 	if err != nil {
// 		panic(err)
// 	}
// 	defer resp.Body.Close()
// 	body, err := ioutil.ReadAll(resp.Body)
// 	index_received, _ := strconv.Atoi(string(body))
// 	m_state.Lock()
// 	if index_received != (server_state.max_index) {
// 		m_state.Unlock()
// 	} else {
// 		m_state.Unlock()
// 		fmt.Println("/ricevuta risposta appendentries")

// 		m_server_log_temp.Lock()
// 		number_commit_values = number_commit_values + 1
// 		half_servers := math.Floor(float64(server_state.server_num)/2) + 1
// 		if number_commit_values == int(half_servers) {
// 			number_commit_values = 0
// 			m_server_log_temp.Unlock()
// 			for i := 0; i < len(*server_addresses); i++ {
// 				go commit_message(server_addresses, (*server_addresses)[i], server_state, term, value)
// 			}
// 		}
// 	}
// }

func http_raft_server(w http.ResponseWriter, r *http.Request) {
	//time.Sleep(1 * time.Second)

	//fmt.Println(mp.server_state.term)

	type_of_message := r.URL.Path
	host := r.Host
	fmt.Println("host")
	fmt.Println(host)
	parameters, _ := url.ParseQuery(r.URL.RawQuery)

	switch type_of_message {
	case "/request_vote":
		term_received, _ := strconv.Atoi(parameters["term"][0])
		log_term_received, _ := strconv.Atoi(parameters["log_term"][0])
		index_received, _ := strconv.Atoi(parameters["index"][0])
		up_to_date := false
		m_state.Lock()
		m_server_log.Lock()
		if server_log[server_state.max_index].term < log_term_received {
			up_to_date = true
		} else {
			if server_log[server_state.max_index].term == log_term_received && index_received >= server_state.max_index {
				up_to_date = true
			}
		}
		m_server_log.Unlock()
		if term_received < server_state.term || term_received == server_state.term_vote_send || !up_to_date {
			m_state.Unlock()
		} else {
			server_state.term_vote_send = term_received
			m_state.Unlock()
			fmt.Println("/request_vote")
			w.Write([]byte(parameters["term"][0]))
		}
	case "/heartbeat":

		term_received, _ := strconv.Atoi(parameters["term"][0])
		id_received := parameters["id"][0]
		index_received, _ := strconv.Atoi(parameters["index"][0])
		log_term_received, _ := strconv.Atoi(parameters["log_term"][0])
		entrie_term_received, _ := strconv.Atoi(parameters["entrie_term"][0])
		entrie_value_received, _ := strconv.Atoi(parameters["entrie_value"][0])
		entrie_client_received := parameters["entrie_client"][0]

		// fmt.Println(term_received)
		fmt.Println(id_received)
		// fmt.Println(index_received)
		// fmt.Println(log_term_received)
		// fmt.Println(entrie_term_received)
		// fmt.Println(entrie_value_received)

		m_state.Lock()
		if term_received < server_state.term {
			m_state.Unlock()
		} else {
			server_state.hb_received = true
			server_state.state = 1
			server_state.term = term_received
			m_state.Unlock()
			if entrie_term_received == -1 {
				// nessuna nuova entry
				w.Write([]byte(parameters["term"][0]))
			} else {
				// nuova entry
				m_server_log.Lock()
				if len(server_log) < index_received+1 {
					// mancano entries richiedere log
					m_server_log.Unlock()
				} else {
					if server_log[index_received].term == log_term_received {
						// apposto aggiungere valore
						server_log = server_log[0:(index_received + 1)]
						server_log = append(server_log, log_atom{entrie_term_received, entrie_value_received, entrie_client_received})
						m_server_log.Unlock()
						w.Write([]byte(parameters["term"][0]))
					} else {
						// problema, richiedere log all'indietro
						m_server_log.Unlock()
					}
				}
			}
		}
	case "/wakeup":
		m_state.Lock()
		server_state.state = 1
		m_state.Unlock()
		fmt.Println("/wakeup")
	case "/newinput":
		fmt.Println("/newinput")
		m_state.Lock()
		term := server_state.term
		if server_state.state == 0 {
			m_state.Unlock()
			value_received, _ := strconv.Atoi(parameters["value"][0])
			m_server_log.Lock()
			server_log = append(server_log, log_atom{term, value_received, host})
			m_server_log.Unlock()
			//w.Write([]byte(parameters["term"][0]))
		} else {
			m_state.Unlock()
			//w.Write([]byte(parameters["term"][0]))
			// indirizzare al leader
		}
	case "/commit":
		fmt.Println("/commit")

		term_received, _ := strconv.Atoi(parameters["term"][0])
		term_to_commit_received, _ := strconv.Atoi(parameters["term_to_commit"][0])
		value_to_commit_received, _ := strconv.Atoi(parameters["value_to_commit"][0])
		index_to_commit_received, _ := strconv.Atoi(parameters["index_to_commit"][0])

		m_state.Lock()
		if term_received < server_state.term {
			//tralascia
			m_state.Unlock()
		} else {
			m_state.Unlock()
			m_server_log.Lock()
			if len(server_log) > index_to_commit_received {
				if server_log[index_to_commit_received].term == term_to_commit_received && server_log[index_to_commit_received].value == value_to_commit_received {
					m_server_log.Unlock()
					m_state.Lock()
					server_state.max_index = server_state.max_index + 1
					m_state.Unlock()
					w.Write([]byte(parameters["term"][0]))
				} else {
					//qualcosa non va, richiedere log
					m_server_log.Unlock()
				}
			} else {
				//manca qualcosa richiedere log
				m_server_log.Unlock()
			}
		}

	default:
		fmt.Println("cosa vuoi")
	}

	//w.Write([]byte(type_of_message))
}

func request_vote_message(server_address string, term int, log_term int, index int) {
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
			fmt.Println("sono il leader")
		}
		m_state.Unlock()
	}
}

func request_vote() {
	for true {
		m_state.Lock()
		if server_state.hb_received || server_state.state == 0 || server_state.state == 3 {
			server_state.hb_received = false
			m_state.Unlock()
		} else {
			server_state.term = server_state.term + 1
			server_state.state = 2
			server_state.term_vote_send = server_state.term
			server_state.term_votes_received = 1
			term := server_state.term
			index := server_state.max_index
			m_server_log.Lock()
			log_term := server_log[index].term
			m_server_log.Unlock()
			m_state.Unlock()
			for i := 0; i < len(server_addresses); i++ {
				go request_vote_message(server_addresses[i], term, log_term, index)
			}
		}
		time.Sleep(time.Duration(rand.Intn(400-300)+300) * time.Millisecond)
	}
}

func commit_message(server_address string, half_servers int, term int, term_to_commit int, value_to_commit int, index_to_commit int) {
	defer recovery()
	client := http.Client{Timeout: 300 * time.Millisecond}
	resp, err := client.Get("http://" + server_address + "/commit?term=" + strconv.Itoa(term) + "&term_to_commit=" + strconv.Itoa(term_to_commit) + "&value_to_commit=" + strconv.Itoa(value_to_commit) + "&index_to_commit=" + strconv.Itoa(index_to_commit))
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	term_received, _ := strconv.Atoi(string(body))
	if term_received == term {
		//possibile commit value
		fmt.Println("/ricevuta risposta commit")
		m_server_log.Lock()
		server_map[server_address][1] = index_to_commit
		number_commit_value := 0
		for i := 0; i < len(server_addresses); i++ {
			if server_map[server_addresses[i]][1] == index_to_commit {
				number_commit_value = number_commit_value + 1
			}
		}
		if number_commit_value == int(half_servers) {
			m_server_log.Unlock()
			m_state.Lock()
			server_state.max_index = server_state.max_index + 1
			m_state.Unlock()
			// dire al client che va bene
		} else {
			m_server_log.Unlock()
		}
	}
}

func heartbeat_message(half_servers int, server_address string, term int, id string, index int, log_term int, entrie_term int, entrie_value int, entrie_client string) {
	defer recovery()
	client := http.Client{Timeout: 300 * time.Millisecond}
	resp, err := client.Get("http://" + server_address + "/heartbeat?term=" + strconv.Itoa(term) + "&id=" + id + "&index=" + strconv.Itoa(index) + "&log_term=" + strconv.Itoa(log_term) + "&entrie_term=" + strconv.Itoa(entrie_term) + "&entrie_value=" + strconv.Itoa(entrie_value) + "&entrie_client=" + entrie_client)
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()
	if entrie_term == -1 {
		// solo heartbeat
	} else {
		//new entry
		body, _ := ioutil.ReadAll(resp.Body)
		term_received, _ := strconv.Atoi(string(body))
		if term_received == term {
			m_server_log.Lock()
			server_map[server_address][0] = index + 1
			number_commit_value := 0
			for i := 0; i < len(server_addresses); i++ {
				if server_map[server_addresses[i]][0] == (index + 1) {
					number_commit_value = number_commit_value + 1
				}
			}
			if number_commit_value == int(half_servers) {
				m_server_log.Unlock()
				for i := 0; i < len(server_addresses); i++ {
					go commit_message(server_address, half_servers, term, entrie_term, entrie_value, index+1)
				}
			}
		}
	}
}

func heartbeat() {
	for true {
		m_state.Lock()
		if server_state.state == 0 {
			id := server_state.id
			term := server_state.term
			index := server_state.max_index
			m_server_log.Lock()
			log_term := server_log[index].term
			entrie := log_atom{-1, -1, "0"}
			if len(server_log) > (server_state.max_index + 1) {
				entrie = server_log[server_state.max_index+1]
				//append (entries, server_log[server_state.next_index])
			}
			m_server_log.Unlock()
			half_servers := math.Floor(float64(server_state.server_num) / 2)
			m_state.Unlock()
			for i := 0; i < len(server_addresses); i++ {
				go heartbeat_message(int(half_servers), (server_addresses)[i], term, id, index, log_term, entrie.term, entrie.value, entrie.client)
			}
		} else {
			m_state.Unlock()
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func main() {
	// lettura indirizzi altri server
	server_map = make(map[string][]int)
	file, err := os.Open("addresses.txt")
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	num_server := 1
	for scanner.Scan() {
		server_addresses = append(server_addresses, scanner.Text())
		server_map[scanner.Text()] = append(server_map[scanner.Text()], 0)
		server_map[scanner.Text()] = append(server_map[scanner.Text()], 0)
		num_server = num_server + 1
	}

	for i := 0; i < len(server_addresses); i++ {
		fmt.Println(server_map[server_addresses[i]][0])
		fmt.Println(server_map[server_addresses[i]][1])
	}

	for i := 0; i < num_server-1; i++ {
		fmt.Println(server_addresses[i])
	}

	//inizializzazione parametri server
	server_state = state{
		id:                  "192.168.1.168",
		id_leader:           "",
		server_num:          num_server,
		state:               3,
		term:                0,
		max_index:           0,
		hb_received:         false,
		term_vote_send:      0,
		term_votes_received: 0}

	server_log = append(server_log, log_atom{0, -1, "127.0.0.1"})

	//task periodici
	go request_vote()
	go heartbeat()

	// inizializzazione server http

	http.HandleFunc("/", http_raft_server)
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
