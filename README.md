# Olive Importer

## Purpose

Import the data from Olive OCR XML files into a canonical JSON format defined by the Impresso project.

## Input data

A sample of the input data for this script can be found in [sample_data/](sample_data/) (data for Gazette de Lausanne (GDL), Feb 2-5 1900).

## Usage

Run the script sequentially:

    python olive_importer.py --input-dir=sample_data/ --output-dir=out/ --temp-dir=tmp/ --verbose --log-file=import_test.log

or in parallel:

    python olive_importer.py --input-dir=sample_data/ --output-dir=out/ --temp-dir=tmp/ --verbose --log-file=import_test.log --parallelize

For further info about the usage, see:

    python olive_importer.py --help

## Notes

- the JSON schemas implemented here are provisional, and should just serve as the basis for discussion

## TODO

- [ ] implement the `info.json` schema
- [ ] deal with `<QW>` elements when extracting box coordinates
- [x] rename: box => region
- [x] implement JSON schema
- [x] add column `article_id` in the case of text box

## JSON Schemas

The output of this script is a bunch of JSON files.

We defined

### Article JSON

The schema is at [schemas/article.schema](schemas/article.schema).

An example:

```json
{
   "legacy": {
      "id": [
         "Ar00103.xml",
         "Ar00200.xml"
      ]
   },
   "meta": {
      "id": "GDL-1900-01-02-a-0004",
      "title": "LETTRE DE PARIS",
      "page_no": [
         1,
         2
      ],
      "publication": {
         "code": "Gazette de Lausanne",
         "label": "GDL"
      },
      "type": {
         "code": "ar",
         "label": "article"
      },
      "language": {
         "code": "fr",
         "label": "french"
      }
   },
   "fulltext": "LETTRE DE PARIS 91. Lavedan \u00e0 l'Acad\u00e9mie-Fran\u00e7aise. Paris, 29 d\u00e9cembre. R\u00e9ception bien \u00ab parisienne \u00bb s'il en fut, que celle deM. Lavedan. L'auteur d\u00fb Prince d'Aurec est un Parisien pur sang, moins peut\u00eatre pourtant que son pr\u00e9d\u00e9cesseur Meilhac, dont l'horizon ne d\u00e9passa jamais la ligne des grands boulevards. Seul, M. Costa de Beauregard est de sa province, ce qui n'est pas un crime, mais ce qui est parfois un gros embarras, quand on se croit oblig\u00e9 \u00e0 faire le badin. Son discours de bienvenue s'en est fortement ressenti, et il a dans\u00e9 un pas d'op\u00e9rette avec des allures de Savoyard. M. Costa de Beauregard, directeur de l'Acad\u00e9mie fran\u00e7aise ! Combien y a-t-il, je ne dis pas de Fran\u00e7ais, mais d'acad\u00e9miciens, qui connaissent les \u0153uvres de M. Costa de Beauregard ? Il a \u00e9t\u00e9 \u00e9lu pour son nom, qui est sonore, pour ses mani\u00e8res, qui sont exquises, et pour son salon qui \u00e9tait, para\u00eet-il, litt\u00e9raire. Ce sont des titr\u00e9s, assur\u00e9ment, mais pourquoi M. de Beauregard ne s'en est-il pas content\u00e9 ? Quelle mouche le pique de vouloir faire lui-m\u00eame de la litt\u00e9rature ? Faire de la litt\u00e9rature, quand on n'est pas dou\u00e9 pour cela ! C'est\u00e0-dire \u00e9carter r\u00e9solument la simplicit\u00e9 de l'expression, parce qu'on la prend pour de la platitude, courir apr\u00e8s l'esprit, attraper g\u00e9n\u00e9ralement la sottise, se torturer le cerveau pour d\u00e9couvrir une \u00e9pith\u00e8te qu'on croit originale, et qui n'est qu'inintelligible, et aboutir, au prix de tant d'efforts, au galimatias triple. M. de Beauregard veut-il dire, par exemple, que M. Lavedan pr\u00eache souvent une morale d'apparence d\u00e9concertante ? \u00ab Je sais bien, \u00e9crira-t-il, que vous rompez \u00e0 vos ouailles un pain de singuli\u00e8re fantaisie. \u00bb Entend-il reprocher \u00e0 son auteur de d\u00e9penser son esprit sur des sujets scabreux ? Il trouvera cette formule mirifique : \u00ab Il vous pla\u00eet de l'envoyer se promener en feux follets sur les pires mar\u00e9cages. \u00bb Ailleurs il aiguisera le trait suivant : \u00ab A ces gentillesses se m\u00ealaient des fantaisies moins \u00e9difiantes o\u00f9 la vie parisienne ne cherchait m\u00eame pas \u00e0 s'excuser de ce qu'elle \u00e9tait et o\u00f9 poup\u00e9es et pantins parlaient en volapuk. \u00bb* Je ne sais pas si ce n'est pas M. cfe Beauregard lui-m\u00eame qui parle en volapuk, mais s\u00fbrement le directeur de l'Acad\u00e9mie fran\u00e7aise ne parle pas en fran\u00e7ais. M. Lavedan, lui, parle en fran\u00e7ais ; ce n'est pas \u00e0 vrai dire le fran\u00e7ais que je pr\u00e9f\u00e8re ; ce n'est pas la langue simple et pure de M\u00e9rim\u00e9e, ou la langue souple et harmonieuse de Renan, ou la langue irr\u00e9prochable, la langue classique de Maupassant. Il y a dans son discours trop de mots, trop d'\u00e9pith\u00e8tes, et une rh\u00e9torique qui d\u00e9tonne avec la mani\u00e8re \u00e2pre, br\u00e8ve, incisive et am\u00e8re de l'auteur du Vieux marcheur. Mais il y a en m\u00eame temps une s\u00e8ve, une verve, une richesse extraordinaires, un vocabulaire d'une vari\u00e9t\u00e9 et d'une \u00e9tendue incroyables, des mots qui font image, et des traits qui vont au but, sans qu'on aper\u00e7oive, comme chez M. de Beauregard, l'effort de l'archer. Le morceau d\u00e9bute par un portrait physique de Meilhac qui est tout \u00e0 fait achev\u00e9 et qu'il faudrait pouvoir citer en entier. Vous verriez passer le grand vaudevilliste, avec \u00ab sa forte taille, le poids de sa lente d\u00e9marche, la discr\u00e9tion courte et parfois eccl\u00e9siastique de son geste, la prudence de ses propos, la bonne gr\u00e2ce un peu gauche de sa politesse, tout ce qu'il y avait \u00e0 la fois et par opposition, de m\u00e9fiant, de tranquille et de secr\u00e8tement narquois sur ce masqu\u00e9 engourdi dans je ne sais quelle puissance somnolente \u00bb. Vous pourriez l'apercevoir, \u00ab la t\u00e8te \u00e0 la porti\u00e8re de sa voiture, massif et doux, placide et r\u00e9fl\u00e9chi,\ftel qu'un Bouddha baign\u00e9 d'indulgence, promenant sur les hommes et les choses de l'heure, au sage petit trot du cheval acad\u00e9mique, son \u00e9ternel coup d'\u0153il inquiet, furtif et captiv\u00e9 \u00bb. Voil\u00e0 le ton, l'allure g\u00e9n\u00e9rale du discours, avec ses qualit\u00e9s et ses d\u00e9fauts. L'abus des \u00e9pith\u00e8tes est ind\u00e9niable ; il y a en dix-neuf dans les deux phrases que je viens de citer ; mais l'adjectif, toujours inattendu, est aussi toujours significatif, toujours color\u00e9, toujours savoureux, et, en somme, toujours juste. Chaque mot est un coup de pinceau : peu \u00e0 peu l'image se d\u00e9gage et s'impose irr\u00e9sistiblement \u00e0 l'esprit. Encore une fois, ce n'est pas la grande tradition de la langue classique, mais s'il fallait chercher des anc\u00eatres au nouvel acad\u00e9micien, ne vous semble-t-il pas que le nom de Saint Simon viendrait naturellement sur les l\u00e8vres ? Je ne poursuis pas l'analyse du discours de M. Lavedan ; je vous dis : lisez-le, et ne manquez pas surtout le morceau de la fin, d'une fantaisie \u00e9tonnante, attendrie, et saugrenue, le couplet sur la modiste. Meilhac, s'\u00e9chappant pour une heure aux d\u00e9lices des Champs-Elys\u00e9es, est revenu dans son ap partement de la place de la Madeleine : une modiste y est install\u00e9e et les petites amies de l'\u00e9crivain, les \u00ab parisiennes \u00bb qu'il a tant aim\u00e9es, sont l\u00e0 qui bavardent en essayant des chapeaux. Il les conseille, il les \u00e9coute, il boit leurs sourires, rit de leurs malices, les reconna\u00eet et les appelle par leurs noms, puis il retourne joyeux \u00e0 la a sombre demeure \u00bb et raconte ce qu'il a vu \u00e0 \u00ab ses amis Labiche et- Marivaux \u00bb. Moli\u00e8re, en les voyant si gais, s'approche .... Ce brillant discours a pass\u00e9 sans encombre, mais on n'\u00e9tait pas sans inqui\u00e9tude. 11 faut vous dire que M. Lavedan est un peu en froid avec l'Acad\u00e9mie ; depuis son \u00e9lection, il a fait jouer le Vieux Marcheur, qui est la plus inconvenante de ses pi\u00e8ces, et probablement de toutes les pi\u00e8ces contemporaines. Il a aggrav\u00e9 sa faute en mettant sur l'affiche son titre d'acad\u00e9micien. Certains immortels se sont montr\u00e9s peu flatt\u00e9s d'\u00eatre compromis dans la soci\u00e9t\u00e9 o\u00f9 se pla\u00eet le h\u00e9ros de la com\u00e9die, le s\u00e9nateur Labosse ; et c'est ce grief qui expliquerait certains passages particuli\u00e8rement agressifs du discours de M. Costa de Beauregard. On dit que ce discours \u00e9tait, \u00e0 l'origine, plus agressif encore, et que le nouvel acad\u00e9micien a d\u00fb demander la suppression de certains passages. Comment M. Lavedan, qui est un homme d'esprit, a-t-il pu s'\u00e9mouvoir des sarcasmes obscurs de son interlocuteur ? N'a-t-il donc pas compris qu'il en serait veng\u00e9... rien qu'en les entendant ? F."
}
```

### info.json

The schema will be at [schemas/article.schema](schemas/info.schema).

TODO: implement
