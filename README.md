# First time cloning project

This project include git sub modules. That's why you need to clone this project with the option --recurse-submodules.

``
git clone --recurse-submodules url
``

If you already cloned this project you can do instead :


```
git submodule init
```

```
git submodule update
```

For more information on git sub modules please see git documentation : https://git-scm.com/book/fr/v2/Utilitaires-Git-Sous-modules

# Modify sub modules

As explained in sub module documentation, to update it you have to go inside the module.
Commit your change.
 
You can rebase with remote repository at any moment :

```
git submodule update --remote --rebase
```

If you do not specify --rebase nor --merge, your changes will not be applied, instead it will swith you with the origin branch.
Your work would still be available on your local branch. Or if it was conflicting it would warn you. 

## Publishing Submodule Changes

Either run if you want the push to fail if submodule are not up to date
```
git push --recurse-submodules=check
```

Either run if you want the push all your work event what you did in the submodule (this will directly update git project of the sub module)
```
git push --recurse-submodules=on-demand
```

# Build project

The run this command in root of this project

```
mvn clean install -DskipTests
```

# Install datasource plugin in your grafana instance

## Requirement

Run this command to be sure to be up to date. 

```
git submodule update
```

If you modified the plugin, working on his own repository and you want to upgrade the datasource in this project run this instead :

```
git submodule update --remote
```

And do not forget to commit the results so that others will got the updated version as well.

## install plugin

You just need to copy the plugin folder **./grafana-historian-dataosurce** folder of the plugin into the plugin folder of your grafana instances.
Look at your grafana.ini file, by default the path is **./data/plugins/**.

So you could do something like
 
 ``` shell script
cp -r ./grafana-historian-dataosurce ${GRAFANA_HOME}/data/plugins/
```

You need to restart your grafana server so that the changes are taking in account.

# Development

Please see our documentation [here](DEVELOPMENT.md)


