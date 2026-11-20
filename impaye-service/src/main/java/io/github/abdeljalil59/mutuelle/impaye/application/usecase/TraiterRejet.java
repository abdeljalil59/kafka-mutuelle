package io.github.abdeljalil59.mutuelle.impaye.application.usecase;

import io.github.abdeljalil59.mutuelle.impaye.application.port.out.PersistanceImpaye;
import io.github.abdeljalil59.mutuelle.impaye.application.port.out.PublicationRelance;
import io.github.abdeljalil59.mutuelle.impaye.domain.Impaye;
import io.github.abdeljalil59.mutuelle.impaye.domain.Relance;
import java.time.Clock;

// Un prélèvement rejeté : on l'enregistre, puis on relance l'adhérent quand ce rejet est le N-ième.
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

    // IDEMPOTENCE. Le nombre de rejets est lu en base (COUNT par adhérent), pas tenu dans un compteur : un résultat
    // rejoué n'ajoute pas de ligne, donc ne gonfle rien. Mais le N-ième rejet rejoué retrouverait un décompte égal au
    // seuil : on s'appuie donc sur le résultat de l'enregistrement, et un doublon ne relance jamais.
    // La relance part quand le décompte est EXACTEMENT égal au seuil, une seule fois par adhérent : les rejets suivants
    // (N+1, N+2...) n'en émettent pas.
    // LIMITE connue (première version). Enregistrement et publication ne sont pas atomiques : si la publication échoue,
    // l'exception remonte et le message est réessayé, mais la livraison suivante trouve l'impayé déjà enregistré (doublon)
    // et ne relance pas. Une relance peut donc être perdue sur une panne de Kafka au mauvais moment.
    public void traiter(Impaye impaye) {
        if (!persistance.enregistrer(impaye)) {
            return;
        }
        long nombreDeRejets = persistance.compterParAdherent(impaye.idAdherent());
        if (nombreDeRejets == seuil) {
            publicationRelance.publier(
                    new Relance(impaye.idAdherent(), nombreDeRejets, impaye.montant(), clock.instant()));
        }
    }
}
