rm -r build/
mkdir build/
javac src/Main.java src/DatabaseInteractions.java -d build/
java -cp build/:resources/postgresql-42.2.14.jar Main