%% The following are binary codes defining the message
%% types for inter dc communication
-define(CHECK_UP_MSG,1).
-define(JOURNAL_READ_REQUEST,2).
-define(OK_MSG,4).
-define(ERROR_MSG,5).
-define(BCOUNTER_REQUEST,6).
-define(HEALTH_CHECK_MSG,7).

%% The number of bytes a partition id is in a message
-define(PARTITION_BYTE_LENGTH, 20).
%% the number of bytes a message id is
-define(REQUEST_ID_BYTE_LENGTH, 2).
-define(REQUEST_ID_BIT_LENGTH, 16).

-define(REQUEST_TYPE_BYTE_LENGTH, 8).

-type inter_dc_message_type() :: ?CHECK_UP_MSG | ?JOURNAL_READ_REQUEST | ?OK_MSG | ?ERROR_MSG | ?BCOUNTER_REQUEST.
