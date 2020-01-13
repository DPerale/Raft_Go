package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"net/http"

	//"net"
	"math/rand"
	"net/url"

	"math"
	//"runtime"
	"strconv"
	"strings"
	"sync"

	// "sync"
	"bufio"
	"os"
	"time"
)

// tipo atomico del log
type log_atom struct {
	term   int
	value  int
	client string
}

// struttura dello stato del server
type state struct {
	id         string
	id_leader  string
	server_num int

	state            int
	term             int
	max_index        int
	max_leader_index int

	hb_received bool

	term_vote_send      int
	term_votes_received int

	in_learning bool
}

// log del server
var server_log []log_atom

// stato del server
var server_state state

// indirizzi degli altri server
var server_addresses []string

// variabili di supporto
var server_map map[string][]int
var m_state sync.Mutex
var m_server_log sync.Mutex

// funzione che permette il recupero senza uscire del tutto dall'applicazione in caso di errore
func recovery() {
	if r := recover(); r != nil {
		fmt.Println("recovered:", r)
	}
}

// messaggio per capire se l'ultima entry del log è coerente con il leader
func learn_log_message(server_address string, last_log_entry_index int, last_log_entry_term int) int {
	defer recovery()
	client := http.Client{Timeout: 300 * time.Millisecond}
	resp, err := client.Get("http://" + server_address + "/learn_log?last_log_entry_term=" + strconv.Itoa(last_log_entry_term) + "&last_log_entry_index=" + strconv.Itoa(last_log_entry_index))
	if err != nil {
		return -1
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	term_received, _ := strconv.Atoi(string(body))
	return term_received
}

// messaggio per richiedere una entry del log del leader
func request_log_entry_message(server_address string, index_to_request int) string {
	defer recovery()
	client := http.Client{Timeout: 300 * time.Millisecond}
	resp, err := client.Get("http://" + server_address + "/request_log_entry?index_to_request=" + strconv.Itoa(index_to_request))
	if err != nil {
		nomess := "a/a/a"
		return nomess
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	message_received := string(body)
	return message_received
}

// imparare il log dal leader
func learn_log() {
	my_last_log_entry_is_ok := false
	// capire quale sia l'ultima entry del log coerente con il leader
	for !my_last_log_entry_is_ok {
		m_state.Lock()
		m_server_log.Lock()
		last_log_entry_index := server_state.max_index
		last_log_entry_term := server_log[last_log_entry_index].term
		m_server_log.Unlock()
		leader_id := server_state.id_leader
		m_state.Unlock()
		term_received := learn_log_message(leader_id, last_log_entry_index, last_log_entry_term)
		m_state.Lock()
		if term_received == last_log_entry_term || server_state.max_index == 0 {
			m_state.Unlock()
			my_last_log_entry_is_ok = true
		} else {
			m_server_log.Lock()
			server_log = server_log[0:server_state.max_index]
			server_state.max_index = server_state.max_index - 1
			m_server_log.Unlock()
			m_state.Unlock()
		}
	}

	// imparare tutte le entry mancanti dall'ultima entry del log coerente con il leader
	i_have_all_log := false
	for !i_have_all_log {
		m_state.Lock()
		index_to_request := server_state.max_index + 1
		leader_id := server_state.id_leader
		m_state.Unlock()
		message_received := request_log_entry_message(leader_id, index_to_request)
		values := strings.Split(message_received, "/")
		if values[0] != "a" {
			m_server_log.Lock()
			entry_term, _ := strconv.Atoi(values[0])
			entry_value, _ := strconv.Atoi(values[1])
			entry_client := values[2]
			server_log = append(server_log, log_atom{entry_term, entry_value, entry_client})
			m_server_log.Unlock()
			m_state.Lock()
			server_state.max_index = server_state.max_index + 1
			fmt.Println(server_log[0 : server_state.max_index+1])
			if server_state.max_index == server_state.max_leader_index {
				i_have_all_log = true
				server_state.in_learning = false
			}
			m_state.Unlock()
		}
	}
}

func http_raft_server(w http.ResponseWriter, r *http.Request) {

	type_of_message := r.URL.Path
	host := r.Host
	parameters, _ := url.ParseQuery(r.URL.RawQuery)

	m_state.Lock()
	// se il server è disattivo controllare se la richiesta è un wakeup altrimenti ignorare tutto
	if server_state.state == 3 {
		if type_of_message == "/wakeup" {
			server_state.state = 1
			fmt.Println("/wakeup")
		}
		m_state.Unlock()
	} else {
		m_state.Unlock()
		switch type_of_message {
		// richiesta di voto da parte di un candidato
		case "/request_vote":
			term_received, _ := strconv.Atoi(parameters["term"][0])
			log_term_received, _ := strconv.Atoi(parameters["log_term"][0])
			index_received, _ := strconv.Atoi(parameters["index"][0])
			up_to_date := false
			m_state.Lock()
			m_server_log.Lock()
			// procedura per capire se il candidato è meno aggiornato dell'attuale server
			if server_log[server_state.max_index].term < log_term_received {
				up_to_date = true
			} else {
				if server_log[server_state.max_index].term == log_term_received && index_received >= server_state.max_index {
					up_to_date = true
				}
			}
			m_server_log.Unlock()
			if term_received < server_state.term || term_received == server_state.term_vote_send || !up_to_date {
				// se il candidato è meno aggiornato, ha un term minore oppure il server ha già votato non si da il voto
				m_state.Unlock()
			} else {
				// i controlli sono andati a buon fine dare il voto
				server_state.term_vote_send = term_received
				m_state.Unlock()
				fmt.Println("/request_vote")
				w.Write([]byte(parameters["term"][0]))
			}
		// heartbeat da parte del leader
		case "/heartbeat":

			term_received, _ := strconv.Atoi(parameters["term"][0])
			id_received := parameters["id"][0]
			index_received, _ := strconv.Atoi(parameters["index"][0])
			log_term_received, _ := strconv.Atoi(parameters["log_term"][0])
			entrie_term_received, _ := strconv.Atoi(parameters["entrie_term"][0])
			entrie_value_received, _ := strconv.Atoi(parameters["entrie_value"][0])
			entrie_client_received := parameters["entrie_client"][0]

			m_state.Lock()
			if term_received < server_state.term {
				m_state.Unlock()
			} else {
				server_state.hb_received = true
				server_state.state = 1
				server_state.term = term_received
				server_state.id_leader = id_received
				server_state.max_leader_index = index_received
				if server_state.in_learning {
					m_state.Unlock()
				} else {
					m_state.Unlock()
					if entrie_term_received == -1 {
						// heartbeat nessuna nuova entry
						m_server_log.Lock()
						if len(server_log) < index_received+1 {
							// mancano entries richiedere log al leader
							m_server_log.Unlock()
							m_state.Lock()
							server_state.in_learning = true
							m_state.Unlock()
							go learn_log()
						} else {
							if server_log[index_received].term == log_term_received {
								// il log è corretto
								m_server_log.Unlock()
							} else {
								// problema nell'ultima entry conosciuta, richiedere log al leader
								m_server_log.Unlock()
								m_state.Lock()
								server_state.in_learning = true
								m_state.Unlock()
								go learn_log()
							}
						}
					} else {
						// heartbeat con nuova entry
						m_server_log.Lock()
						if len(server_log) < index_received+1 {
							// mancano entries richiedere log al leader
							m_server_log.Unlock()
							m_state.Lock()
							server_state.in_learning = true
							m_state.Unlock()
							go learn_log()
						} else {
							if server_log[index_received].term == log_term_received {
								// aggiungere valore, i controlli sono andati a buon fine
								server_log = server_log[0:(index_received + 1)]
								server_log = append(server_log, log_atom{entrie_term_received, entrie_value_received, entrie_client_received})
								m_server_log.Unlock()
								w.Write([]byte(parameters["term"][0]))
							} else {
								// problema nell'ultima entry conosciuta, richiedere log al leader
								m_server_log.Unlock()
								m_state.Lock()
								server_state.in_learning = true
								m_state.Unlock()
								go learn_log()
							}
						}
					}
				}
			}
		// richiesta di un nuovo valore da salvare da parte di un client
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
				w.Write([]byte("0"))
			} else {
				leader_id := server_state.id_leader
				fmt.Println(leader_id)
				m_state.Unlock()
				w.Write([]byte(leader_id))
				// indirizzare il client al leader
			}
		// richiesta di commit da parte del leader
		case "/commit":
			fmt.Println("/commit")

			term_received, _ := strconv.Atoi(parameters["term"][0])
			term_to_commit_received, _ := strconv.Atoi(parameters["term_to_commit"][0])
			value_to_commit_received, _ := strconv.Atoi(parameters["value_to_commit"][0])
			index_to_commit_received, _ := strconv.Atoi(parameters["index_to_commit"][0])

			m_state.Lock()
			if term_received < server_state.term || server_state.in_learning || index_to_commit_received <= server_state.max_index {
				// messaggio inviato a server sbagliato, tralascia
				m_state.Unlock()
			} else {
				m_state.Unlock()
				m_server_log.Lock()
				if len(server_log) > index_to_commit_received {
					if server_log[index_to_commit_received].term == term_to_commit_received && server_log[index_to_commit_received].value == value_to_commit_received {
						m_server_log.Unlock()
						m_state.Lock()
						server_state.max_index = server_state.max_index + 1
						m_server_log.Lock()
						fmt.Println(server_log[0 : server_state.max_index+1])
						m_server_log.Unlock()
						m_state.Unlock()
						w.Write([]byte(parameters["term"][0]))
					} else {
						//i valori del commit e quelli posseduti sono diversi, richiedere log al leader
						m_server_log.Unlock()
						m_state.Lock()
						server_state.in_learning = true
						m_state.Unlock()
						go learn_log()
					}
				} else {
					//manca qualcosa richiedere log al leader
					m_server_log.Unlock()
					m_state.Lock()
					server_state.in_learning = true
					m_state.Unlock()
					go learn_log()
				}
			}
		// richiesta da parte di un server di
		case "/learn_log":
			entry_term_received, _ := strconv.Atoi(parameters["last_log_entry_term"][0])
			entry_index_received, _ := strconv.Atoi(parameters["last_log_entry_index"][0])
			m_state.Lock()
			m_server_log.Lock()
			if server_state.state != 0 || server_log[entry_index_received].term != entry_term_received {
				//tralascia
				m_state.Unlock()
				m_server_log.Unlock()
				w.Write([]byte(strconv.Itoa(-1)))
			} else {
				m_state.Unlock()
				m_server_log.Unlock()
				w.Write([]byte(strconv.Itoa(entry_term_received)))
			}
		// richiesta da parte di un server di una parte del log
		case "/request_log_entry":
			//time.Sleep(50 * time.Millisecond)
			index_received, _ := strconv.Atoi(parameters["index_to_request"][0])
			m_state.Lock()
			if server_state.state != 0 {
				m_state.Unlock()
				w.Write([]byte("a/a/a"))
			} else {
				m_state.Unlock()
				m_server_log.Lock()
				entry_term := strconv.Itoa(server_log[index_received].term)
				entry_value := strconv.Itoa(server_log[index_received].value)
				entry_client := server_log[index_received].client
				m_server_log.Unlock()
				w.Write([]byte(entry_term + "/" + entry_value + "/" + entry_client))
			}
		// procedura per fermare momentaneamente il server
		case "/sleep":
			m_state.Lock()
			server_state.state = 3
			m_state.Unlock()
		// richiesta da parte del client di chi è il leader
		case "/leader":
			m_state.Lock()
			leader_id := server_state.id_leader
			m_state.Unlock()
			w.Write([]byte(leader_id))
		// richiesta da parte del client del log
		case "/log_for_client":
			fmt.Println("log_for_client")
			all_the_log := ""
			for i := 0; i < server_state.max_index+1; i++ {
				term := strconv.Itoa(server_log[i].term)
				value := strconv.Itoa(server_log[i].value)
				client := server_log[i].client
				all_the_log = all_the_log + "/" + term + "/" + value + "/" + client + "/"
			}
			w.Write([]byte(all_the_log))
		default:
			fmt.Println(type_of_message)
			fmt.Println("messaggio non identificato")
		}
	}
}

// messaggio di richiesta di voto agli altri server
func request_vote_message(server_address string, term int, log_term int, index int) {
	defer recovery()
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
			m_server_log.Lock()
			server_log = server_log[0 : server_state.max_index+1]
			m_server_log.Unlock()
			server_state.state = 0
			server_state.id_leader = server_state.id
			fmt.Println("sono il leader")
		}
		m_state.Unlock()
	}
}

// richiesta di candidatura a leader in caso non si riceva l'heartbeat dopo un certo tempo
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

// messaggio di commit, lanciato a tutti i server se la maggior parte ha ricevuto il nuovo valore
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
		//possibile commit del valore, se c'è la maggioranza
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
			m_server_log.Lock()
			fmt.Println(server_log[0 : server_state.max_index+1])
			client_address := server_log[server_state.max_index].client
			client_value := server_log[server_state.max_index].value
			m_server_log.Unlock()
			m_state.Unlock()
			// dire al client che il valore è stato aggiunto
			client = http.Client{Timeout: 300 * time.Millisecond}
			resp, err = client.Get("http://" + client_address + "/response_ok?value=" + strconv.Itoa(client_value))
			if err != nil {
				panic(err)
			}
			defer resp.Body.Close()
		} else {
			m_server_log.Unlock()
		}
	}
}

// costruzione e invio messaggio di heartbeat ad un server
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
					go commit_message((server_addresses)[i], half_servers, term, entrie_term, entrie_value, index+1)
				}
			} else {
				m_server_log.Unlock()
			}
		}
	}
}

// procedura di heartbeat che può inserire nuovi commit
func heartbeat() {
	for true {
		//fmt.Print("num goroutine: ")
		//fmt.Println(runtime.NumGoroutine())
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
	fmt.Println("Inizializzazione Server")
	server_map = make(map[string][]int)
	file, err := os.Open("addresses.txt")
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	num_server := 1
	scanner.Scan()
	server_id := scanner.Text()
	for scanner.Scan() {
		server_addresses = append(server_addresses, scanner.Text())
		server_map[scanner.Text()] = append(server_map[scanner.Text()], 0)
		server_map[scanner.Text()] = append(server_map[scanner.Text()], 0)
		num_server = num_server + 1
	}
	for i := 0; i < num_server-1; i++ {
		fmt.Println(server_addresses[i])
	}

	// inizializzazione parametri server
	server_state = state{
		id:                  server_id,
		id_leader:           "0.0.0.0",
		server_num:          num_server,
		state:               3,
		term:                0,
		max_index:           0,
		max_leader_index:    0,
		hb_received:         false,
		term_vote_send:      0,
		term_votes_received: 0,
		in_learning:         false}

	server_log = append(server_log, log_atom{0, -1, "127.0.0.1"})

	// goroutine periodiche
	go request_vote()
	go heartbeat()

	// inizializzazione server http
	http.HandleFunc("/", http_raft_server)
	if err := http.ListenAndServe(":80", nil); err != nil {
		fmt.Println(err)
	}
}
