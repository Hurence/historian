# First time cloning project

This project include git sub modules. That's why you need to clone this project with the option --recurse-submodules.

``
git clone --recurse-submodules url
``

If you aleady cloned this project you can do instead :


```
git submodule init
```

```
git submodule update
```

For more information on git sub modules please see git documentation.

# Build project

The run this command in root of this project

```
mvn clean install -DskipTests
```


