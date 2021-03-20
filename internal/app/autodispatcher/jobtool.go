package autodispatcher

import (
	"encoding/json"
)

func ParkJobConf(jobConf *JobConf) (value []byte, err error) {

	value, err = json.Marshal(jobConf)
	return
}
func UParkJobConf(value []byte) (jobConf *JobConf, err error) {

	jobConf = new(JobConf)
	err = json.Unmarshal(value, jobConf)
	return
}

func ParkGroupConf(groupConf *GroupConf) (value []byte, err error) {

	value, err = json.Marshal(groupConf)
	return
}

func UParkGroupConf(value []byte) (groupConf *GroupConf, err error) {

	groupConf = new(GroupConf)
	err = json.Unmarshal(value, groupConf)
	return
}

func ParkJobSnapshot(snapshot *JobSnapshot) (value []byte, err error) {

	value, err = json.Marshal(snapshot)
	return
}

func UParkJobSnapshot(value []byte) (snapshot *JobSnapshot, err error) {

	snapshot = new(JobSnapshot)
	err = json.Unmarshal(value, snapshot)
	return
}

func ParkJobExecuteSnapshot(snapshot *JobExecuteSnapshot) (value []byte, err error) {
	value, err = json.Marshal(snapshot)
	return

}

func UParkJobExecuteSnapshot(value []byte) (snapshot *JobExecuteSnapshot, err error) {

	snapshot = new(JobExecuteSnapshot)
	err = json.Unmarshal(value, snapshot)
	return
}

