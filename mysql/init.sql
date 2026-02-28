CREATE DATABASE IF NOT EXISTS dataplateform;
USE dataplateform;

-- =====================================
-- 1. SUPPRESSION (si existant)
-- =====================================
DROP TABLE IF EXISTS paiement, facture, ligne_commande, commande, ligne_devis, devis, stock, produit, categorie, commercial, client CASCADE;

-- =====================================
-- 2. CREATION DES TABLES
-- =====================================

CREATE TABLE client (
    id_client SERIAL PRIMARY KEY,
    code_client VARCHAR(50) UNIQUE,
    raison_sociale VARCHAR(255),
    adresse TEXT,
    telephone VARCHAR(20),
    email VARCHAR(100),
    date_creation TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE commercial (
    id_commercial SERIAL PRIMARY KEY,
    nom VARCHAR(100),
    prenom VARCHAR(100),
    email VARCHAR(100),
    telephone VARCHAR(20)
);

CREATE TABLE categorie (
    id_categorie SERIAL PRIMARY KEY,
    nom_categorie VARCHAR(100)
);

CREATE TABLE produit (
    id_produit SERIAL PRIMARY KEY,
    nom_produit VARCHAR(255),
    description TEXT,
    prix_unitaire DECIMAL(10,2),
    id_categorie INT REFERENCES categorie(id_categorie)
);

CREATE TABLE stock (
    id_stock SERIAL PRIMARY KEY,
    id_produit INT REFERENCES produit(id_produit),
    quantite INT,
    seuil_alerte INT
);

CREATE TABLE devis (
    id_devis SERIAL PRIMARY KEY,
    id_client INT REFERENCES client(id_client),
    id_commercial INT REFERENCES commercial(id_commercial),
    date_devis DATE,
    statut VARCHAR(50),
    montant_total DECIMAL(12,2)
);

CREATE TABLE ligne_devis (
    id_ligne SERIAL PRIMARY KEY,
    id_devis INT REFERENCES devis(id_devis),
    id_produit INT REFERENCES produit(id_produit),
    quantite INT,
    prix_unitaire DECIMAL(10,2)
);

CREATE TABLE commande (
    id_commande SERIAL PRIMARY KEY,
    id_devis INT REFERENCES devis(id_devis),
    date_commande DATE,
    statut VARCHAR(50),
    montant_total DECIMAL(12,2)
);

CREATE TABLE ligne_commande (
    id_ligne SERIAL PRIMARY KEY,
    id_commande INT REFERENCES commande(id_commande),
    id_produit INT REFERENCES produit(id_produit),
    quantite INT,
    prix_unitaire DECIMAL(10,2)
);

CREATE TABLE facture (
    id_facture SERIAL PRIMARY KEY,
    id_commande INT REFERENCES commande(id_commande),
    date_facture DATE,
    montant_total DECIMAL(12,2),
    statut VARCHAR(50)
);

CREATE TABLE paiement (
    id_paiement SERIAL PRIMARY KEY,
    id_facture INT REFERENCES facture(id_facture),
    date_paiement DATE,
    montant DECIMAL(12,2),
    mode_paiement VARCHAR(50)
);

-- =====================================
-- 3. INSERTION DES DONNEES
-- =====================================

-- Clients
INSERT INTO client (code_client, raison_sociale, adresse, telephone, email)
VALUES
('CL001', 'SENEGAL LOGISTICS', 'Dakar', '770000001', 'contact@senlog.sn'),
('CL002', 'AFRICA MINING', 'Thiès', '770000002', 'contact@mining.sn'),
('CL003', 'BATIMENT SAHEL', 'Saint-Louis', '770000003', 'contact@batiment.sn');

-- Commerciaux
INSERT INTO commercial (nom, prenom, email, telephone)
VALUES
('Ndiaye', 'Moussa', 'moussa@neemba.sn', '770111111'),
('Fall', 'Awa', 'awa@neemba.sn', '770222222');

-- Catégories
INSERT INTO categorie (nom_categorie)
VALUES
('Engins'),
('Pièces détachées');

-- Produits
INSERT INTO produit (nom_produit, description, prix_unitaire, id_categorie)
VALUES
('Bulldozer CAT D6', 'Engin de chantier', 50000000, 1),
('Pelle hydraulique', 'Excavatrice', 75000000, 1),
('Filtre moteur', 'Pièce détachée', 25000, 2);

-- Stock
INSERT INTO stock (id_produit, quantite, seuil_alerte)
VALUES
(1, 5, 1),
(2, 3, 1),
(3, 100, 10);

-- Devis
INSERT INTO devis (id_client, id_commercial, date_devis, statut, montant_total)
VALUES
(1, 1, '2026-01-10', 'accepté', 50025000),
(2, 2, '2026-01-12', 'en_cours', 75000000);

-- Lignes devis
INSERT INTO ligne_devis (id_devis, id_produit, quantite, prix_unitaire)
VALUES
(1, 1, 1, 50000000),
(1, 3, 1, 25000),
(2, 2, 1, 75000000);

-- Commande (issue du devis accepté)
INSERT INTO commande (id_devis, date_commande, statut, montant_total)
VALUES
(1, '2026-01-15', 'validée', 50025000);

-- Lignes commande
INSERT INTO ligne_commande (id_commande, id_produit, quantite, prix_unitaire)
VALUES
(1, 1, 1, 50000000),
(1, 3, 1, 25000);

-- Facture
INSERT INTO facture (id_commande, date_facture, montant_total, statut)
VALUES
(1, '2026-01-16', 50025000, 'en_attente');

-- Paiement
INSERT INTO paiement (id_facture, date_paiement, montant, mode_paiement)
VALUES
(1, '2026-01-20', 50025000, 'virement');
