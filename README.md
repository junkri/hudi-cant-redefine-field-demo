# hudi-cant-redefine-field-demo
Demo project for triggering "can't redefine field" error in Hudi

### Run the demo

```shell
./mvnw clean package

./mvnw exec:exec -Ddemo=1
./mvnw exec:exec -Ddemo=2
```

### Demo 1
Edge case when we have two `decimal` fields with the same fieldname, but in different places (in different structures)
and with different precision.

### Demo 2
We have a `decimal` and a `record` fields with the same fieldname, but in different places.

### Loading project to IDE

Please add the following jvm arguments when running the project:
```shell
--add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.lang.invoke=ALL-UNNAMED
```