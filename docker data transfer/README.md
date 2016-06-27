# Docker Cron Container: dashDB to Object Storage
A simple docker container that runs a cron job to saves data from rashDB to Bluemix Object Store in the parquet format using spark-submit.sh

## how to install and use it
First copy the repository.

Open the file *spark/vcap.json* and insert your Spark credentials. (you may first need to create service credentials in the Spark dashboard).
```
{
  "credentials": {
    "tenant_id": "",
    "tenant_id_full": "",
    "cluster_master_url": "",
    "instance_id": "",
    "tenant_secret": "",
    "plan": ""
  }
}
```

Open the *spark/pi.py* file that is executed by spark-submit.sh. Edit the following:

```
# paste the credentials of DashDB
credentialsDDB = {
  'port':'',
  'db':'',
  'username':'',
  'ssljdbcurl':'',
  'host':'',
  'https_url':'',
  'dsn':'',
  'hostname':'',
  'jdbcurl':'',
  'ssldsn':'',
  'uri':'',
  'password':''
}
```
```
# change the tablename according to the table in DashDB you want to use
tablename = "MYTABLE";
```
```
# paste the credentials of Object Store
# check that there is an attribute "name" with the value "spark in the credentials, else add
oscredentials = {
    "name": "spark",
    "auth_url": "",
    "project": "",
    "projectId": "",
    "region": "",
    "userId": "",
    "username": "",
    "password": "",
    "domainId": "",
    "domainName": ""
}
```
```
# change container name (my-container) and file name to be saved (simdata.parquet)
df.write.parquet("swift://my-container.spark/simdata.parquet", mode="overwrite")
```

You must have a running Docker installation, the CF CLI and the CF IC plugin on your client:

https://docs.docker.com/engine/installation/).

https://docs.cloudfoundry.org/cf-cli/install-go-cli.html

https://console.ng.bluemix.net/docs/containers/container_cli_ov.html

Now we can build the Dockerfile, register the image in the Bluemix registry and start running the container:

```
$ cf login
$ cf ic login
$ docker build -t image_name
$ docker tag image_name registry.ng.bluemix.net/user/image_name:latest
$ docker push registry.ng.bluemix.net/user/image_name:latest
$ cf ic run --name container_name registry.ng.bluemix.net/user/image_name:latest
```


To check if it is running properly, connect to the container using the id and view the logfile. (You may have to wait 2 minutes)

```
$ cf ic exec -it my_container_id bash
root@b149b5e7306d:/# cat /var/log/cron.log
Thu May 26 13:11:01 UTC 2016: executed script
Thu May 26 13:12:01 UTC 2016: executed script
```
Also check the *spark-submit.sh* logs. You can find them in */spark-logs*.
```
root@b149b5e7306d:/# ls /spark-logs
spark-submit_1464166853765599100.log stderr_1464166853765599100 stdout_1464166853765599100
```
Finally, go to the Object Store Dashboard (by clicking on your Object Store Service), there you should see the parquet files.
The cron job is running and the we have parquet files in our Bluemix Object Store!

## how to modify
To change the interval the cron job is runned, just simply edit the *crontab* file. In default, the job is runned every 10 minutes.

```
*/10 * * * * root /spark/spark-submit.sh --deploy-mode cluster --vcap /spark/vcap.json /spark/pi.py
*/10 * * * * root /spark/script.sh
```
