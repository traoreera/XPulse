# Intégration XFlow — Plugin XPulse

Système de notifications temps réel haute performance basé sur Redis Pub/Sub et SSE.

## ⚡ Actions IPC

| Action | Qualified Name | Entrée (Payload) | Sortie |
| :--- | :--- | :--- | :--- |
| **Stream** | `XPulse.stream` | `StreamPayload` | `{"data": {"status": "ok", "channels": array}}` |

---

## 📦 Détail des Objets (Schemas)

### `StreamPayload`
- `channels`: (array[string], optionnel) Liste des channels sur lesquels publier (ex: `["notifications", "tasks"]`).
- `channel`: (string, optionnel) Channel unique si `channels` est absent.
- `event`: (object, requis) L'objet notification à envoyer.
  - Structure recommandée :
    - `user_id`: (string) ID du destinataire pour filtrage SSE.
    - `type`: (string) Catégorie de l'event (ex: `new_message`).
    - `title`: (string) Titre court.
    - `text`: (string) Contenu du message.

## 📡 Événements (Event Bus)

- `ext.notification.publish` (Écouté) : `{ "channels": array, ...data }`.
- `ext.notification.broadcast` (Écouté) : `{ "text": string, "channels": array }` Diffuse à tous les utilisateurs enregistrés auprès d'Auth.
- `auth.get.user.ids` (Émis) : Utilisé pour résoudre les destinataires d'un broadcast.
