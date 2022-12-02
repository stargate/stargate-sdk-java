# Stargate Software Development Kit

[![License Apache2](https://img.shields.io/hexpm/l/plug.svg)](http://www.apache.org/licenses/LICENSE-2.0)

This page is mean to help developer to release this the SDK.

## Prerequisites

- You need to have `Java 8+` and Maven installed.


- You need to have an `ssh` key setup to work with github. Here is some [documentation](https://docs.github.com/en/authentication/connecting-to-github-with-ssh/adding-a-new-ssh-key-to-your-github-account)


- You need to be able to sign the artifacts with `gpg`. As such you will need to create and distribute a new GPG key. Here is a procedure on how to do it. [documentation](https://dzone.com/articles/how-to-publish-artifacts-to-maven-central)


- You need to setup a server `ossrh` in your maven configuration file `settings.xml`. The credentials has been provoided to you when your register to the maven central


```xml
<?xml version="1.0" encoding="UTF-8"?>
<settings xmlns="http://maven.apache.org/SETTINGS/1.0.0" 
          xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" 
          xsi:schemaLocation="http://maven.apache.org/SETTINGS/1.0.0 http://maven.apache.org/xsd/settings-1.0.0.xsd">
  
    <proxies></proxies>

    <servers>
    
     <server>
       <id>ossrh</id>
       <username>your_username</username>
       <password>your_password</password>
     </server>
    </servers>
```


### Release Procedure

- Start the ssh agent on your machine

```bash
eval "$(ssh-agent -s)"
```

- Add the ssh key to the agent

```
cd ~/.ssh
ssh-add my_github_key
```

- Cleanup Make sure you local copy is the same as github.  

```bash
mvn clean
find . -type f -name *.DS_Store -ls -delete
git pull
git add -A
git commit -m "Cleanup before a release"
git push
```

- Make sure the code is working.

```bash
mvn clean install
```

- Start a release

```bash
mvn release:prepare
```

- If something goes wrong and you need to retry

```bash
git tag -d my_tag
git push --delete origin my_tag
mvn release:rollback
mvn release:clean
```

- if everything is OK

```bash
mvn release:perform
```

- Now Connect to [Maven Central](https://oss.sonatype.org/#stagingRepositories) to close and release your Staging repository

- When the release is successful convert your github [tag](https://github.com/stargate/stargate-sdk-java/tags) to a real release.

```bash
https://github.com/stargate/stargate-sdk-java/releases/new?tag=my_tag
```





