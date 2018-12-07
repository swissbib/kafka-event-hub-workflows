

1 erste Gedanken beim Einarbeiten kafka-event-hub / kafka-event-hub-workflows

@Jonas / @Sebastian (zur Diskussion)
* ich finde den Ansatz gut, workflows und Funktionalität zu trennen
* hast Du ihn aber wirklich bis zu Ende geführt?  In den Packages digispace / dsv01 / dsv05 / zum Teil user_data und 
utility gibt es eine Steuerung des workflows aber auch Model-Funktionalität (Zugriffe auf Elasticsearch, Transformationen, etc)
Ist das so gewollt?
* Du hast für das "Eliasprojekt" natürlich sehr viele Quellen zusammenbringen und vor dem Zusammenbringen aufbereiten müssen.
Ich frage mich im Moment, ob wir nicht zuviele Dinge "zusammenpacken"
Im Moment habe ich das Ziel, den contentCollector mit seinen bestehenden workflows zu vereinfachen, indem ich versuche, 
diese workflows zu entflechten und den EventHub Kafka in das Zentrum zu stellen.
Durch Kafka sollen die workflows einfacher werden
* Ich habe ein wenig Bedenken, dass wir durch ein zentrales workflow Modell wieder ein grosses Gebilde aufbauen
* Unser Ziel ist es, Mikroservices in container zu verpacken. Schaffen wir das mit einem workflow repository, in dem 
zum Teil unterschiedliche Use cases integriert werden?     
Können wir darüber mal reden?
* Einen kurzen Moment habe ich bei dem Gedanken workflow an mögliche Tools für dieses Thema gedacht. was mir dabei in 
den Sinn kommt sind Stichworte wie Luigi oder Nifi. Dann habe ich den Gedanken aber schon fast wieder verworfen
weil ich nicht jede Sache mit einem "Tool erschlagen" möchte. Was kommt Euch dazu in den Sinn?

2 wie werde ich weiter vorgehen

* das von Jonas erstellte noch ein wenig genauer ansehen.
* die contentCollector Funktionalität anfangen Schritt für Schritt zu übertragen
* ich fände es aber gut, wenn wir die oben geäusserten Gedanken gleich nächste Woche diskutieren können. Am Montag bin
 ich leider nicht da (Freiburg, Bernd). Können wir Dienstag miteinander sprechen? Wollt Ihr Eure Gedanken vorher vielleicht schon ein wenig festhalten?   
 
