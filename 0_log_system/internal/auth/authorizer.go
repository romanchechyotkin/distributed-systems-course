package auth

import (
	"fmt"

	"github.com/casbin/casbin"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Authorizer struct {
	enforcer *casbin.Enforcer
}

func New(model, policy string) *Authorizer {
	enforcer := casbin.NewEnforcer(model, policy)
	return &Authorizer{enforcer: enforcer}
}

func (a *Authorizer) Authorize(sub, obj, action string) error {
	if !a.enforcer.Enforce(sub, obj, action) {
		msg := fmt.Sprintf("%s not permitted to %s to %s", sub, action, obj)
		st := status.New(codes.PermissionDenied, msg)
		return st.Err()
	}

	return nil
}
