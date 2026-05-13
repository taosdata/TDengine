package controller

import (
	"testing"

	"github.com/gin-gonic/gin"
)

type MockController struct{}

func (mc MockController) Init(_ gin.IRouter) {}

func TestAddAndGetControllers(t *testing.T) {
	mockController := MockController{}
	AddController(mockController)
	cs := GetControllers()
	if len(cs) != 1 {
		t.Errorf("Expected number of controllers to be 1, but got %d", len(cs))
	}
	if _, ok := cs[0].(MockController); !ok {
		t.Error("Expected the controller to be an instance of MockController")
	}
}
