package controllers

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/astaxie/beego"
	"github.com/astaxie/beego/logs"
	"github.com/vesoft-inc/nebula-http-gateway/service/copier"
	"github.com/vesoft-inc/nebula-http-gateway/service/es"
	"github.com/vesoft-inc/nebula-http-gateway/service/importer"
	"github.com/vesoft-inc/nebula-importer/pkg/config"

	importerErrors "github.com/vesoft-inc/nebula-importer/pkg/errors"
)

type TaskController struct {
	beego.Controller
}

type ImportRequest struct {
	ConfigPath string            `json:"configPath"`
	ConfigBody config.YAMLConfig `json:"configBody"`
}

type ImportActionRequest struct {
	TaskID     string `json:"taskID"`
	TaskAction string `json:"taskAction"`
}

type CopyRequest struct {
	SrcSpace      string `json:"src_space"`
	DstSpace      string `json:"dst_space"`
	Force         bool   `json:"force"`
	PartitionNum  int    `json:"partition_num"`
	ReplicaFactor int    `json:"replica_factor"`
	VidType       string `json:"vid_type"`
	Debug         bool   `json:"debug"`
	BatchSize     int    `json:"batch_size"`
}

type SyncESRequest struct {
	Space      string `json:"space"`
	ESIndex    string `json:"es_index"`
	BatchSize  int    `json:"batch_size"`
	ESUsername string `json:"es_username"`
	ESPassword string `json:"es_password"`
}

func (this *TaskController) Import() {
	var (
		res    Response
		params ImportRequest
		taskID string = importer.NewTaskID()
		err    error
	)

	task := importer.NewTask(taskID)
	importer.GetTaskMgr().PutTask(taskID, &task)

	err = json.Unmarshal(this.Ctx.Input.RequestBody, &params)

	if err != nil {
		err = importerErrors.Wrap(importerErrors.InvalidConfigPathOrFormat, err)
	} else {
		err = importer.Import(taskID, params.ConfigPath, &params.ConfigBody)
	}

	if err != nil {
		// task err: import task not start err handle
		task.TaskStatus = importer.StatusAborted.String()
		logs.Error(fmt.Sprintf("Failed to start a import task: `%s`, task result: `%v`", taskID, err))

		res.Code = -1
		res.Message = err.Error()
	} else {
		res.Code = 0
		res.Data = []string{taskID}
		res.Message = fmt.Sprintf("Import task %s submit successfully", taskID)
	}
	this.Data["json"] = &res
	this.ServeJSON()
}

func (this *TaskController) ImportAction() {
	var res Response
	var params ImportActionRequest

	json.Unmarshal(this.Ctx.Input.RequestBody, &params)
	result, err := importer.ImportAction(params.TaskID, importer.NewTaskAction(params.TaskAction))
	if err == nil {
		res.Code = 0
		res.Data = result
		res.Message = "Processing a task action successfully"
	} else {
		res.Code = -1
		res.Message = err.Error()
	}
	this.Data["json"] = &res
	this.ServeJSON()
}

func (this *TaskController) Copy() {
	var res Response
	var params CopyRequest

	nsid := this.GetSession(beego.AppConfig.String("sessionkey"))
	if nsid == nil {
		res.Code = -1
		res.Message = "connection refused for lack of session"
		this.Data["json"] = &res
		this.ServeJSON()
		return
	}

	err := json.Unmarshal(this.Ctx.Input.RequestBody, &params)
	if err != nil {
		res.Code = -1
		res.Message = "Invalid request body: " + err.Error()
		this.Data["json"] = &res
		this.ServeJSON()
		return
	}

	if params.SrcSpace == "" || params.DstSpace == "" {
		res.Code = -1
		res.Message = "src_space and dst_space are required"
		this.Data["json"] = &res
		this.ServeJSON()
		return
	}

	// Generate task ID and create task
	taskID := importer.NewTaskID()
	task := importer.NewTask(taskID)
	importer.GetTaskMgr().PutTask(taskID, &task)

	// Start async copy in goroutine
	go func() {
		ctx := context.Background()
		batchSize := params.BatchSize
		if batchSize <= 0 {
			batchSize = beego.AppConfig.DefaultInt("copyBatchSize", 1000)
		}
		err := copier.CopySpace(ctx, nsid.(string), params.SrcSpace, params.DstSpace, params.Force, params.PartitionNum, params.ReplicaFactor, params.VidType, params.Debug, batchSize)
		if err != nil {
			logs.Error(fmt.Sprintf("Failed to copy space: `%s` -> `%s`, error: `%v`", params.SrcSpace, params.DstSpace, err))
			task.TaskStatus = importer.StatusAborted.String()
			task.TaskMessage = err.Error()
		} else {
			task.TaskStatus = importer.StatusFinished.String()
			importer.GetTaskMgr().DelTask(taskID)
			logs.Debug(fmt.Sprintf("Success to copy space: `%s` -> `%s`", params.SrcSpace, params.DstSpace))
		}
	}()

	// Return immediately with task ID
	res.Code = 0
	res.Data = []string{taskID}
	res.Message = fmt.Sprintf("Copy task %s submit successfully", taskID)
	this.Data["json"] = &res
	this.ServeJSON()
}

func (this *TaskController) CopyAction() {
	var res Response
	var params ImportActionRequest

	json.Unmarshal(this.Ctx.Input.RequestBody, &params)

	taskAction := importer.NewTaskAction(params.TaskAction)

	// Handle query action
	if taskAction == importer.ActionQuery {
		if t, ok := importer.GetTaskMgr().GetTask(params.TaskID); ok {
			res.Code = 0
			res.Data = []importer.Task{*t}
			res.Message = "Query task successfully"
		} else {
			// Task may have finished and been removed from memory
			res.Code = 0
			res.Data = []importer.Task{{TaskID: params.TaskID, TaskStatus: importer.StatusFinished.String()}}
			res.Message = "Task not in memory, may have finished"
		}
		this.Data["json"] = &res
		this.ServeJSON()
		return
	}

	// Handle stop action
	if taskAction == importer.ActionStop {
		if ok := importer.GetTaskMgr().StopTask(params.TaskID); ok {
			res.Code = 0
			res.Message = "Task stop successfully"
		} else {
			res.Code = -1
			res.Message = "Task has stopped or finished"
		}
		this.Data["json"] = &res
		this.ServeJSON()
		return
	}

	res.Code = -1
	res.Message = "Unknown action"
	this.Data["json"] = &res
	this.ServeJSON()
}

func (this *TaskController) SyncES() {
	var res Response
	var params SyncESRequest

	nsid := this.GetSession(beego.AppConfig.String("sessionkey"))
	if nsid == nil {
		res.Code = -1
		res.Message = "connection refused for lack of session"
		this.Data["json"] = &res
		this.ServeJSON()
		return
	}

	err := json.Unmarshal(this.Ctx.Input.RequestBody, &params)
	if err != nil {
		res.Code = -1
		res.Message = "Invalid request body: " + err.Error()
		this.Data["json"] = &res
		this.ServeJSON()
		return
	}

	if params.Space == "" || params.ESIndex == "" {
		res.Code = -1
		res.Message = "space and es_index are required"
		this.Data["json"] = &res
		this.ServeJSON()
		return
	}

	batchSize := params.BatchSize
	if batchSize <= 0 {
		batchSize = 1000
	}

	// 异步执行
	taskID := importer.NewTaskID()
	task := importer.NewTask(taskID)
	importer.GetTaskMgr().PutTask(taskID, &task)

	go func() {
		syncer, err := es.NewSyncer(nsid.(string), params.Space, params.ESIndex, batchSize, params.ESUsername, params.ESPassword)
		if err != nil {
			logs.Error("Failed to create syncer: %v", err)
			task.TaskStatus = importer.StatusAborted.String()
			task.TaskMessage = err.Error()
			return
		}

		ctx := context.Background()
		count, err := syncer.Sync(ctx)
		if err != nil {
			logs.Error("Failed to sync: %v", err)
			task.TaskStatus = importer.StatusAborted.String()
			task.TaskMessage = err.Error()
		} else {
			task.TaskStatus = importer.StatusFinished.String()
			task.TaskMessage = fmt.Sprintf("Synced %d documents", count)
		}
	}()

	res.Code = 0
	res.Data = map[string]interface{}{
		"task_id": taskID,
	}
	res.Message = fmt.Sprintf("Sync task %s submitted successfully", taskID)
	this.Data["json"] = &res
	this.ServeJSON()
}
