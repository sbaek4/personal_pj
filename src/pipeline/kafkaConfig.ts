/**
 * Kafka 연결 설정을 한곳에서 만듭니다.
 *
 * Kafka는 "브로커(broker)"라는 서버들이 메시지(이벤트)를 저장·전달합니다.
 * Producer는 토픽(topic)에 메시지를 넣고, Consumer는 같은 토픽을 구독해서 꺼내 읽습니다.
 * 브로커 주소는 환경 변수로 바꿀 수 있어서, 로컬/스테이징/운영 서버를 코드 수정 없이 전환할 수 있습니다.
 */
import { Kafka } from "kafkajs";

/** 콤마로 구분된 브로커 목록 (예: "localhost:9092" 또는 "host1:9092,host2:9092") */
export function getKafkaBrokers(): string[] {
  return (process.env.KAFKA_BROKERS ?? "localhost:9092")
    .split(",")
    .map((value) => value.trim())
    .filter(Boolean);
}

/**
 * 애플리케이션마다 clientId를 다르게 두면 Kafka 쪽 모니터링에서 구분하기 쉽습니다.
 * groupId는 Consumer 그룹 이름입니다. 같은 groupId를 쓰는 인스턴스들이 메시지를 나눠서 처리합니다.
 */
export function createKafkaClient(clientId: string): Kafka {
  return new Kafka({ clientId, brokers: getKafkaBrokers() });
}
