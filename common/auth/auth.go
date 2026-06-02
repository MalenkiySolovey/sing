package auth

import (
	"sync"

	"github.com/sagernet/sing/common"
)

type User struct {
	Username string
	Password string
}

type Authenticator struct {
	userMap map[string][]string

	mu sync.RWMutex
}

func NewAuthenticator(users []User) *Authenticator {
	if len(users) == 0 {
		return nil
	}
	au := &Authenticator{
		userMap: make(map[string][]string),
	}
	for _, user := range users {
		au.userMap[user.Username] = append(au.userMap[user.Username], user.Password)
	}
	return au
}

func (au *Authenticator) UpdateUsers(users []User) {
	userMap := make(map[string][]string)
	for _, user := range users {
		userMap[user.Username] = append(userMap[user.Username], user.Password)
	}
	au.mu.Lock()
	au.userMap = userMap
	au.mu.Unlock()
}

func (au *Authenticator) Verify(username string, password string) bool {
	au.mu.RLock()
	passwordList, ok := au.userMap[username]
	au.mu.RUnlock()
	return ok && common.Contains(passwordList, password)
}
