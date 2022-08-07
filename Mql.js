//*** FIND:*/ 

//1. Afficher les entreprises avec succursales au Canada: 608
db.companies.find({"offices.country_code": "CAN"},{"_id":0, "name":1, "offices.country_code":1})
            .sort({name:1})


/****AGGREGATES: */
//1 Entreprises par categorie (type d'affaire)
db.companies.aggregate([[{
 $match: {}
}, {
 $group: {
  _id: '$category_code',
  total: {
   $sum: 1
  }
 }
}]])

//2 Prix moyen des entreprises, triees par annee de constitution ("founded_year")
db.companies.aggregate([[{
    $match: {
     'acquisition.price_amount': {
      $ne: null
     }
    }
   }, {
    $group: {
     _id: '$founded_year',
     count: {
      $sum: 1
     },
     avgPrice: {
      $avg: '$acquisition.price_amount'
     }
    }
   }, {
    $sort: {
     _id: 1
    }
    }]])

   //3 Entreprises dont l'employee avec le titre "CEO and Founder" ou "CEO and Co-Founder" est encore actif.  Triage par tranche des 10 annees.
   db.companies.aggregate([ [{
    $match: {
     'acquisition.price_amount': {
      $ne: null
     }
    }
   }, {
    $bucket: {
     groupBy: '$founded_year',
     boundaries: [
      1800,
      1900,
      1910,
      1920,
      1930,
      1940,
      1950,
      1960,
      1970,
      1980,
      1990,
      2000,
      2010,
      2020
     ],
     'default': 'Other',
     output: {
      count: {
       $sum: 1
      },
      avgPrice: {
       $avg: '$acquisition.price_amount'
      }
     }
    }
   }] ])

   //4 A partir de la requete precedente, nous l'avons modifie et sauvegarde, comme collection interesante, sous le nom "activeCEOS"
   db.companies.aggregate([ [{
    $match: {
     'relationships.is_past': false,
     'relationships.title': {
      $in: [
       'CEO and Founder',
       'CEO and Co-Founder'
      ]
     }
    }
   }, {
    $unwind: {
     path: '$relationships'
    }
   }, {
    $match: {
     'relationships.is_past': false,
     'relationships.title': {
      $in: [
       'CEO and Founder',
       'CEO and Co-Founder'
      ]
     }
    }
   }, {
    $project: {
     _id: 0,
     name: 1,
     'relationships.is_past': 1,
     'relationships.title': 1,
     'relationships.person': 1
    }
   }, {
    $out: 'activeCEOS'
   }] ])

   //5 Jointure avec l'operateeur "lookup" avec la collection "historicalStock" en prenant l'anne d' introduction a la bourse "ipo.pub_year" comme cle de jointure
   [{
    db.companies.aggregate([ 
     $match: {
     'ipo.pub_year': {
      $exists: true
     }
    }
   }, {
    $lookup: {
     from: 'historicalStock',
     localField: 'ipo.pub_year',
     foreignField: 'Year',
     as: 'year_id'
    }
   }, {
    $project: {
     _id: 0,
     image: 0,
     products: 0,
     relationships: 0,
     competitions: 0,
     providerships: 0,
     funding_rounds: 0,
     investments: 0,
     acquisition: 0,
     acquisitions: 0,
     offices: 0,
     milestones: 0,
     video_embeds: 0,
     screenshots: 0,
     external_links: 0,
     partners: 0
    }
   }] ])
