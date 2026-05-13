package controller

import "github.com/gin-gonic/gin"

type WebController interface {
	Init(r gin.IRouter)
}

var controllers []WebController

func AddController(controller WebController) {
	controllers = append(controllers, controller)
}

func GetControllers() []WebController {
	return controllers
}
