mkdir tmp; cd frontend-maven-plugin
git clone https://github.com/OpenPOWER-BigData/frontend-maven-plugin.git
mvn clean install -DskipTests
cd ..
rm -rf frontend-maven-plugin
mvn clean install -Dtar -DskipTests -Dhadoop.version="2.7.1-ppc64le" -Phadoop27
