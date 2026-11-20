package io.github.abdeljalil59.mutuelle.impaye.application.usecase;

import io.github.abdeljalil59.mutuelle.impaye.application.port.out.PersistanceImpaye;
import io.github.abdeljalil59.mutuelle.impaye.application.port.out.PublicationRelance;
import io.github.abdeljalil59.mutuelle.impaye.domain.Impaye;
import io.github.abdeljalil59.mutuelle.impaye.domain.Relance;
import java.time.Clock;

// Un prélèvement rejeté : on l'enregistre, puis on relance l'adhérent dès que le nombre de ses rejets atteint le seuil.
public class TraiterRejet {

    private final PersistanceImpaye persistance;
    private final PublicationRelance publicationRelance;
    private final Clock clock;
    private final int seuil;

    public TraiterRejet(PersistanceImpaye persistance, PublicationRelance publicationRelance, Clock clock, int seuil) {
        if (seuil < 1) {
            throw new IllegalArgumentException("le seuil de relance doit être au moins 1 : " + seuil);
        }
        this.persistance = persistance;
        this.publicationRelance = publicationRelance;
        this.clock = clock;
        this.seuil = seuil;
    }

    // La relance se décide sur l'ÉTAT EN BASE, à chaque message, nouveau ou rejoué : décompte >= seuil ET aucune relance
    // déjà émise pour cet adhérent. L'enregistrement reste idempotent (un doublon n'ajoute ni ligne ni décompte), mais il ne
    // court-circuite plus la décision : c'est ce qui permet de rattraper une relance dont la publication a échoué, au
    // réessai du même message comme au rejet suivant de l'adhérent.
    //
    // ORDRE : publier PUIS marquer. Si la publication échoue, rien n'est marqué : l'exception remonte (le gestionnaire
    // d'erreurs réessaie) et la relance sera retentée. Si le marquage échoue après une publication réussie, le réessai
    // republie : le pire cas est une relance EN DOUBLE (livraison au moins une fois), jamais une relance perdue. Le
    // consommateur de impayes.relances doit donc être idempotent : une relance n'a pas d'identifiant d'événement, il
    // dédoublonnerait sur idAdherent.
    //
    // UNE RELANCE PAR ADHÉRENT : une fois le marquage posé, les rejets suivants (N+1, N+2...) n'en émettent plus, et un
    // rejet déjà traité qu'on rejoue non plus. nombreRejets est le décompte au moment de l'émission, dernierMontant celui du
    // message qui l'émet (le N-ième en marche normale, un message ultérieur ou rejoué si la relance est rattrapée).
    //
    // MONO-INSTANCE : la base est une H2 en mémoire propre au processus (D9), et « vérifier puis publier puis marquer » n'est
    // pas atomique. Avec la clé idAdherent, un seul consommateur traite un adhérent à la fois, hors rééquilibrage ; deux
    // instances auraient de toute façon chacune leur base, donc leur propre décompte.
    public void traiter(Impaye impaye) {
        persistance.enregistrer(impaye);
        String idAdherent = impaye.idAdherent();
        long nombreDeRejets = persistance.compterParAdherent(idAdherent);
        if (nombreDeRejets >= seuil && !persistance.relanceEmise(idAdherent)) {
            publicationRelance.publier(new Relance(idAdherent, nombreDeRejets, impaye.montant(), clock.instant()));
            persistance.marquerRelanceEmise(idAdherent);
        }
    }
}
