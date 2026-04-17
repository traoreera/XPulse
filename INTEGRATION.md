# Intégration XCore - XPulse

![XCore Support](https://img.shields.io/badge/XCore_Support-2.1.1-6f42c1?style=flat-square&logo=github)
![Security Mode](https://img.shields.io/badge/Mode-Trusted-success?style=flat-square)

Cette documentation détaille comment le plugin **XPulse** interagit avec le noyau et les autres plugins de l'écosystème **XCore**.

## 🧩 Type de Plugin
XPulse est configuré en mode **`trusted`** (défini dans `plugin.yaml`). Ce mode est nécessaire car le plugin gère des connexions persistantes (SSE) et nécessite un accès direct au pool de connexions Redis pour garantir les performances.

## 📡 Bus d'Événements (Event Bus)

XPulse écoute et réagit aux événements globaux suivants :

### Événements Écoutés
- **`ext.notification.publish`** : Publie un message sur un ou plusieurs channels spécifiques.
    - *Payload* : `{ "channels": ["..."], "text": "...", "user_id": "..." }`
- **`ext.notification.broadcast`** : Diffuse un message à tous les utilisateurs identifiés dans le système.
    - *Action interne* : Émet `auth.get.user.ids` pour récupérer la liste des destinataires.

### Événements Émis
- **`auth.get.user.ids`** : Utilisé lors d'un broadcast pour récupérer tous les IDs utilisateurs actifs auprès du plugin d'authentification.

## ⚡ Actions Disponibles

Le plugin expose une action XCore directe utilisable par les autres plugins via le SDK :

### `xpulse.stream`
Permet à un autre plugin d'injecter manuellement un événement dans le flux Redis.
- **Usage on other plugin** :
  ```python
    await self.call_plugin("xpulse.stream", {
      "channels": ["system_notification"],
      "event": { "user_id": "123", "text": "Alerte système" }
  })
  ```

## 🔒 Sécurité et Signatures
En tant que plugin `trusted`, XPulse possède un fichier `plugin.sig`. 
Toute modification du code source dans `src/` invalidera la signature. Pour re-signer le plugin après modification :
```bash
xcore plugin sign ./<your_plugin_directory>/xpulse --key  <your plugin_key>
```

## 🔗 Dépendances
XPulse nécessite le plugin suivant pour fonctionner correctement :
- **`auth`** (version `>=0.1.0, <0.3.0`) : Utilisé pour la résolution des identifiants utilisateurs lors des broadcasts.
