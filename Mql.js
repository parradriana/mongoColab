//*** FIND: */////

//1. Afficher les enterprises avec succursales au Canada: 
db.companies.find({ "offices.country_code": "CAN" }, { "_id": 0, "name": 1, "offices.country_code": 1 }).pretty();

//2. Afficher les entreprises dont la categorie (type d'affaire) est "analytics": 
db.companies.find({ ategory_code: { $exists: true, $eq: "analytics" } }, { _id: 0, name: 1, category_code: 1 }).pretty();

////3 Afficher le nom, code de catégorie et code de pays des entreprises dont la catégorie est « analytics» et qui ont des sucursales au Canada :
db.companies.find({ "category_code": "analytics", "offices.country_code": "CAN" }, { _id: 0, "name": 1, "category_code": 1, "offices.country_code": 1 });

////3.1 Nombre d' entreprises avec sucursales au Canada:
db.companies.count({ "offices.country_code": "CAN" });

//4 Nom des entreprises qui ont des bureaux au Montréal ou Toronto
db.companies.find(
    {
        $or: [
            { "offices.city": "Toronto" }, { "offices.city": { $in: ["Montreal", "Montréal"] } }]
    },
    { "_id": 0, "name": 1, "offices.city": 1 }
);

////4.1 Combien d' entreprises ont des bureaux a Montreal:
db.companies.count({ "offices.city": { $in: ["Montreal", "Montréal"] } });

//5 Sélectionner le nom et ladresse des entreprises de taille moyenne (qui comptent de 100 à 499 employés) et qui ont des bureaux à Montréal, triées par nom
db.companies.find({
    $and: [{ "offices.city": { $in: ["Montreal", "Montréal"] } }
        , { number_of_employees: { $gt: 99 } }, { number_of_employees: { $lt: 499 } }]
}
    , { _id: 0, name: 1, "offices.address1": 1, "offices.address2": 1 }).sort({ names: 1 });

//6

/****AGGREGATES: */////

//1 Afficher le nombre d'entreprises avec sucursales au Canada (requete qui a le meme resultat (608) que la requete 3.1)
db.companies.aggregate([[{
    $match: {
        'offices.country_code': 'CAN'
    }
}, {
    $count: 'country_code'
}]]);

//2 Entreprises par categorie (type d'affaire)
db.companies.aggregate([[{
    $match: {}
}, {
    $group: {
        _id: '$category_code',
        total: {
            $sum: 1
        }
    }
}]]);

//3 Prix moyen des entreprises, triees par annee de constitution ("founded_year")
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
}]]);

//4 Entreprises dont l'employee avec le titre "CEO and Founder" ou "CEO and Co-Founder" est encore actif.  Triage par tranche des 10 annees.
db.companies.aggregate([[{
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
}]])

//5 A partir de la requete precedente, nous l'avons modifie et sauvegarde, comme collection interesante, sous le nom "activeCEOS"
[{
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
    }]

//6 Jointure avec l'operateeur "lookup" avec la collection "historicalStock" en prenant l'anne d' introduction a la bourse "ipo.pub_year" comme cle de jointure
[{
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
    }]
