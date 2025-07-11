# Vérifications à réaliser durant le processus de traitement des fichiers diplômés

## I. Vérifications
### 1. a priori
- on vérifie que les valeurs des variables d'intérêt peuvent exister par rapport aux données de la BCN (types de bac, de diplômes, de disciplines,...)
- on vérifie la cohérence entre la formation suivie et le diplôme (diplôme final et intermédiaire la même année, diplôme intermédiaire pour formation qui n'en a pas,...)
- on vérifie les années et les sources de données (u0, u2, u3 et u4)

### 2. a posteriori
- on vérifie à nouveau la cohérence entre la formation suivie et le diplôme (diplôme final et intermédiaire la même année, diplôme intermédiaire pour formation qui n'en a pas,...)
- utilise le fichier verification_diplomes.csv pour créer le fichier evolutions_importantes_sans_diplomr.xlsx à l'aide du programme u5
- dans evolutions_importantes_sans_diplomr.xlsx, on regarde :
     * si les valeurs manquantes sont cohérentes par rapport aux dates d'ouverture et de fermeture des établissements
     * si les variations importantes sont cohérentes avec les données de base (fichiers non traités)


## II. Mise à jour de la Google sheet
- [!IMPORTANT] on rajoute les UAI et ID Paysage par année et jeu de données (result, inge,...) dans la table A_UAI [!IMPORTANT]
- on arjoute éventuellement des informations dans les autres onglets, dont des UAI dans ETABLI_SOURCE, des diplômes dans deleter,...

## III. Documentation
- on télécharge od_diplomes.csv
- on note le nombre de lignes
- on fait les tableaux dans Tableau (voir classeur tableau_doc_diplomes ou Python) :
    * Types d'établissement
    * Typologies d'établissement
    * Âge au bac
    * Bac
    * Diplômes
    * Disciplines du diplôme
    * INSPE/ESPE
- on fait tourner le programme u6 pour générer la liste des établissements avec leur ID Paysage et noms d'origine, leur ID Paysage et nom actuel, le rattachement des établissements-composantes et grands établissements à leur EPE
