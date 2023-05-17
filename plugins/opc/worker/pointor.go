package worker

import (
	"collector/common"
	"collector/connector"
	"collector/connector/opcda"
	"collector/connector/opcua"
	"context"
	"fmt"
	"log"
)

type Pointer interface {
	GetAllPoints(ctx context.Context) ([]common.Point, error)
	Exist(ctx context.Context)
}

type OpcPointer struct {
	connector connector.Connector
}

var _ Pointer = (*OpcPointer)(nil)

func NewOpcPointer(config common.Config) (pointer Pointer, err error) {
	var c connector.Connector
	if config.OpcType == common.OpcTypeUA {
		config.Collect.Ua = common.UaCollectConfig{} // don't need collecting config when get all points
		c, err = opcua.NewConnector(config)
	}
	if config.OpcType == common.OpcTypeDA {
		config.Collect.Da = common.DaCollectConfig{} // don't need collecting config when get all points
		c, err = opcda.NewConnector(config)
	}
	if err != nil {
		return
	}
	if c == nil {
		return nil, fmt.Errorf("unknown opc type %s", config.OpcType)
	}

	pointer = &OpcPointer{connector: c}
	return
}

func (p *OpcPointer) GetAllPoints(ctx context.Context) ([]common.Point, error) {
	if err := p.connector.Connect(ctx); err != nil {
		log.Println("## connect opc error ", err)
		return nil, err
	}
	return p.connector.GetAllPoints(ctx)
}

func (p *OpcPointer) Exist(ctx context.Context) {
	p.connector.Stop(ctx)
}
