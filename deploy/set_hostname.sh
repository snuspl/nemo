echo "./set_hostname.sh remote_node_name"
echo "TODO: should use pssh, rather than ssh"
HOST=$1
ssh $HOST "sudo bash -c 'echo $HOST > /etc/hostname'"
