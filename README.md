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

# Development

Please see our documentation [here](DEVELOPMENT.md)


