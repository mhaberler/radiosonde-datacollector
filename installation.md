


## Installation
I assume a debian server.

The code has been tested only with Python 3.8. A working nginx installation is needed which includes the [ngx-brotli](https://github.com/google/ngx_brotli) and [webdav](http://nginx.org/en/docs/http/ngx_http_dav_module.html) modules.  Also, install lftp:

    apt install lftp

## Prerequisites
Create a user which owns the files, and runs the update job:

    # adduser --system --shell /bin/bash radiosonde
    # adduser radiosonde www-data

Install conda as [per instructions](https://docs.conda.io/projects/conda/en/latest/user-guide/install/linux.html) from [here](https://docs.conda.io/en/latest/miniconda.html#linux-installers).
wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh
bash Miniconda3-latest-Linux-x86_64.sh
. .bashrc

and create a conda environment:

    conda create --name radiosonde python=3.8
    conda activate radiosonde

Add this to the radiosonde user's .bash_profile:

    source ~/miniconda3/etc/profile.d/conda.sh
    conda activate radiosonde

Exit, and su - radiosonde; the radiosonde user should come up with the proper conda environment:

     # su - radiosonde
    (radiosonde) radiosonde@cloud:~$

## Deployment by git push

I did the installation [following this gist.](https://gist.github.com/noelboss/3fe13927025b89757f8fb12e9066f2fa#file-post-receive). The idea is to create a bare repo and deploy the code to a separate directory with a post-receive hook.

```
mkdir ~/radiosonde-deploy
git init --bare ~/radiosonde-datacollector.git
```
Add the following file as ~/radiosonde-datacollector.git/hooks/post-receive:

    #!/bin/bash
    TARGET="/home/radiosonde/radiosonde-deploy"
    GIT_DIR="/home/radiosonde/radiosonde-datacollector.git"
    BRANCH="master"

    while read oldrev newrev ref
    do
    	# only checking out the master (or whatever branch you would like to deploy)
    	if [ "$ref" = "refs/heads/$BRANCH" ];
    	then
    		echo "Ref $ref received. Deploying ${BRANCH} branch to production..."
    		git --work-tree=$TARGET --git-dir=$GIT_DIR checkout -f $BRANCH
    	else
    		echo "Ref $ref received. Doing nothing: only the ${BRANCH} branch may be deployed on this server."
    	fi
    done

 and make it executable:
```
chmod +x ~/radiosonde-datacollector.git/hooks/post-receive
```
On your local server, add a remote to point to the bare repo and push (you might have to do the usual ssh incantations to enable ssh'ing into the radiosonde account):
```
$ cd ~/path/to/working-copy/
$ git remote add production ssh://radiosonde@yourserver.com:radiosonde-datacollector.git
# and push to it
$ git push production master
```
Verify that the push resulted in a checked-out tree in ~/radiosonde-deploy .

## Web server directory layout
assuming /var/www/radiosonde as root:

    data/madis/
    data/gisc/
    static/
    app/

data and and subdirectories plus static must be writable by the radiosonde user.

Deploy the station_list.txt file:

	cp station_list.txt /var/www/radiosonde/static/

## Spool directory layout
Create the following directories - must be writable by the radiosonde user:

    /var/spool/madis/incoming
    /var/spool/madis/processed
    /var/spool/madis/failed
    /var/spool/gisc/incoming
    /var/spool/gisc/processed
    /var/spool/gisc/failed

## Data feeds

## MADIS:

Review the [/home/radiosonde/radiosonde-deploy/update-madis.sh](https://github.com/mhaberler/radiosonde-datacollector/blob/master/update-madis.sh) script and edit as needed. I keep the edited version under /home/radiosonde/update-madis.sh so it is not overwritten by the next push.

Run it manually the first time - as a result, the  /var/spool/madis/incoming directory should be populated by files looking like `20210222_0000.gz` etc. These are gzipped netCDF files containing the soundings from MADIS.

Add a crontab entry to run this script every now and then:

    /etc/cron.d/radiosonde:57,27 *     * * *     radiosonde   /home/radiosonde/update-madis.sh

## GISC:

GISC offers a variety of delivery mechanisms. I use https put to my server which delivers zip archives of BUFR-formatted files to /var/spool/gisc/incoming .

Configure nginx to accept https push with basic authentication to /incoming . The nginx config fragment looks like so:

    # incoming location for gisc.dwd.de https push delivery
    location /incoming {
        autoindex on;
        root /var/spool/gisc/;
        client_body_temp_path /var/spool/gisc/tmp;
        dav_methods PUT;
        create_full_put_path off;
        dav_access group:rw all:r;

        if (-d $request_filename) {
            rewrite ^(.*[^/])$ $1/ break;
        }

        auth_basic "Username and Password Required";
        auth_basic_user_file /etc/nginx/htpasswd/htpasswd.gisc;
    }

Verify you can https push to /incoming using basic auth:

    curl -H 'Content-Type: application/zip' \
        --user $USER:$PASSWORD \
        -X PUT -T <some local file>  \
        https://yourserver.com/incoming/testfile

Create an account on https://gisc.dwd.de/wisportal/# and log in.
Under Subscriptions -> Internet Subscriptions - Edit destinations add a destination like so:

![Create a destination](https://static.mah.priv.at/public/gisc.jpg)

Try this out by clicking 'Test HTTP(S) PUT destination'  .  The result should be a file being deposited to  /var/spool/gisc/incoming .

Now add a subscription:  Click  Subscriptions -> Default carts and scroll down to 'TEMP_Data-global_FM94' . Select that and associate it with the subscription destination added above.

After a few minutes files should begin to be delivered to /var/spool/gisc/incoming.

## Running ingest.sh

Copy ~/radiosonde-deploy/ingest.sh to /home/radiosonde and edit as needed.

Run ingest.sh manually with the '-v' option for verbose output.

Add a crontab entry to run as desired:

    /etc/cron.d/radiosonde:15,30,45,0 *     * * *     radiosonde   /home/radiosonde/ingest.sh


## Files created

After running a while, the following files should appear:

    /var/radiosonde/data/summary.geojson.br
    /var/radiosonde/static/station_list.json
    /var/radiosonde/data/madis/<xx>/yy/<basename>.geojson.br
    ...
    /var/radiosonde/data/gisc/<xx>/yy/<basename>.geojson.br
    ...
