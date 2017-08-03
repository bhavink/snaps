# snaps
Repo for SnapLogic custom snaps built using SnapLogic SDK http://developer.snaplogic.com

In order to create a demo project, think of it like a jump-start java project which has several snaps available

Try this on CLI, you’ll need JDK and Maven, which I assume you already have.

mvn org.apache.maven.plugins:maven-archetype-plugin:2.4:generate -DarchetypeCatalog=http://maven.clouddev.snaplogic.com:8080/nexus/content/repositories/master/


Use these options to pass on and this will give you a demo project.
 
1
1.8
groupId: com.snaplogic.snaps
artifactId: your-snappack-name
version: 4.9.0.M1
package: com.snaplogic.snaps.jsondiff
organization: snaplogic
assetPath: /snaplogic/shared
snapPack: jsondiff
user: bkukadia
Y: :
 
mvn clean install -DskipTests -DVERSION=1 -Dsl_build=00

With every build you change -Dsl_build number for ex:

mvn clean install -DskipTests -DVERSION=1 -Dsl_build=0001
mvn clean install -DskipTests -DVERSION=1 -Dsl_build=0002

And so on

