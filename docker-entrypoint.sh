#!/bin/sh
#!/bin/bash
actCode=$1
case "${actCode}" in
  start_server)
    cmd="/app/job-scheduler-server"
    config_path="/usr/local/etc/config.properties"
    cat ${config_path}

    args="-configure=$config_path"
    echo "$cmd $args"
    /bin/sh -c "$cmd $args"
    ;;
  start_exec)
    cmd="/app/job-scheduler-exec"
        config_path="/usr/local/etc/config.properties"
        cat ${config_path}

        args="-configure=$config_path"
        echo "$cmd $args"
        /bin/sh -c "$cmd $args"
        ;;
  *)
    echo "actCode=${actCode} is invalid." > /dev/stderr
    exit 1
    ;;
esac