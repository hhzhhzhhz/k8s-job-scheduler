package errors

func NewError(code int64, cause string) Error {
	return Error{code, cause}
}

type Error struct {
	Code  int64
	Cause string
}

// params
var (
	InvalidParameter   = NewError(100, "invalid parameter.")
	JobIdError         = NewError(101, "job_id is empty.")
	BodyReadError      = NewError(102, "http body read failed.")
	BodyDecodeError    = NewError(103, "http body decode failed.")
	BodyMarshalError   = NewError(104, "json marshal failed.")
	VerifyJobError     = NewError(105, "verify Job failed.")
	AlreadyExistsError = NewError(106, "data already exists.")
	ServerClosedError  = NewError(107, "server is closing.")
	ForwarderUrlError  = NewError(108, "forwarder url parse failed.")
)

// mysql
var (
	SqlCreateError    = NewError(150, "db insert failed.")
	SqlUnChangedError = NewError(151, "db data unchanged.")
	SqlUpdateError    = NewError(152, "db update failed.")
	SqlQueryError     = NewError(153, "db query failed.")
)

// business
var (
	PublishError          = NewError(251, "publish message failed.")
	GenJobError           = NewError(252, "generate job failed.")
	DeleteJobTimeoutError = NewError(253, "delete job timeout.")
	JobSubmittedError     = NewError(254, "job Submitted")
	DeleteJobError        = NewError(255, "delete job error.")
)
