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

# Modify sub modules

As explained in sub module documentation, to update it you have to go inside the module.
Commit your change.
 
You can rebase with remote repository at any moment :

```
git submodule update --remote --rebase
```

If you do not specify --rebase nor --merge, your changes will not be applyed but still on the branch you were.
Or if it was conflicting it would warn you. 

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



weekly team 10/02/2020

- loader : 
    - BENOIT / PR du chargement des données coservit en mode générique
    - TOM / review PR & merge
    - TOM / chargement 1 mois de data pour démo
- gateway : 
    - GREG / merge PR du filtrage du nom des métriques
    - FEIZ / implem de la recherche des annotations
- analytic :
    - MEJD / intég de la fonction de seuil SAX dans la lib timeseries + unit tests
    - MEJD / integ de la lib grammar viz
    - TOM / utilisation de la fonction seuils SAX pour indexer des annotations
