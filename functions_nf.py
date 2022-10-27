from kds.functions_bubble import get_compra_by_num
from kds.functions_internal import compra_lookup
from kds.config import Config

import pymongo
import xmltodict
import datetime
from pynfe.processamento.comunicacao import ComunicacaoSefaz
from pynfe.entidades.cliente import Cliente
from pynfe.entidades.emitente import Emitente
from pynfe.entidades.notafiscal import NotaFiscal
from pynfe.entidades.fonte_dados import _fonte_dados
from pynfe.processamento.serializacao import SerializacaoXML, SerializacaoQrcode # noqa
from pynfe.processamento.assinatura import AssinaturaA1
from pynfe.utils.flags import CODIGO_BRASIL
from decimal import Decimal

client = pymongo.MongoClient(Config.atlas_access)


def NF_total():
    pipe = [
        {
            '$group':
                {
                    '_id': '_id',
                    'total': {'$sum': "$dados_compra.valor_total"},
                }
        }
       ]
    print(list(client.grammo.nf.aggregate(pipe)))


def NF_grammo(dados_compra, dados_cliente=False):

    certificado = "certificado.pfx"
    senha = Config.senha_certificado
    uf = 'rs'
    # Algumas UF's não possuem homologação para este serviço
    homologacao = False

    # emitente
    emitente = Emitente(
        razao_social='Grammo Comércio de Alimentos Ltda',
        nome_fantasia='Grammo Brasil',
        cnpj='41602084000110',           # cnpj apenas números
        codigo_de_regime_tributario='1', # 1 para simples nacional ou 3 para normal # noqa
        inscricao_estadual='0963861174', # numero de IE da empresa # noqa
        # inscricao_municipal='12345',
        cnae_fiscal='5320202',           # cnae apenas números
        endereco_logradouro='Ipiranga',
        endereco_numero='3435',
        endereco_bairro='Partenon',
        endereco_municipio='Porto Alegre',
        endereco_uf='RS',
        endereco_cep='90610001',
        endereco_pais=CODIGO_BRASIL
    )

    # Nota Fiscal
    nota_fiscal = NotaFiscal(
       emitente=emitente,
       uf=uf.upper(),
       natureza_operacao='VENDA', # venda, compra, transferência, devolução, etc # noqa
       forma_pagamento=0,         # 0=Pagamento à vista; 1=Pagamento a prazo; 2=Outros. # noqa
       tipo_pagamento=1,
       modelo=65,                 # 55=NF-e; 65=NFC-e
       serie='1',
       numero_nf = dados_compra['nf_num'],           # Número do Documento Fiscal. # noqa
       data_emissao=datetime.datetime.now(),
       data_saida_entrada=datetime.datetime.now(),
       tipo_documento=1,          # 0=entrada; 1=saida
       municipio='4314902',       # Código IBGE do Município
       tipo_impressao_danfe=4,    # 0=Sem geração de DANFE;1=DANFE normal, Retrato;2=DANFE normal Paisagem;3=DANFE Simplificado;4=DANFE NFC-e; # noqa
       forma_emissao='1',         # 1=Emissão normal (não em contingência);
       cliente_final=1,           # 0=Normal;1=Consumidor final;
       indicador_destino=1,
       indicador_presencial=1,
       finalidade_emissao='1',    # 1=NF-e normal;2=NF-e complementar;3=NF-e de ajuste;4=Devolução de mercadoria. # noqa
       processo_emissao='0',      #0=Emissão de NF-e com aplicativo do contribuinte; # noqa
       transporte_modalidade_frete=9, # 9=Sem Ocorrência de Transporte. # noqa
       informacoes_adicionais_interesse_fisco='DOCUMENTO EMITIDO POR ME OU EPP OPTANTE PELO SIMPLES NACIONAL NÃO GERA DIREITO A CRÉDITO FISCAL DE IPI', # noqa
       totais_tributos_aproximado=Decimal('0'),
    )

    # cliente
    if dados_cliente:
        cliente = Cliente(
            razao_social=dados_cliente['nome'],
            tipo_documento='CPF',           #CPF ou CNPJ # noqa
            email=dados_cliente['email'],
            numero_documento=dados_cliente['CPF'], # numero do cpf ou cnpj # noqa
            indicador_ie=9,                 # 9=Não contribuinte
            #endereco_logradouro=dados_cliente['logradouro'],
            #endereco_numero=dados_cliente['numero'],
            #endereco_complemento=dados_cliente['complemento'],
            #endereco_bairro=dados_cliente['bairro'],
            #endereco_municipio=dados_cliente['municipio'],
            #endereco_uf=dados_cliente['UF'],
            #endereco_cep=dados_cliente['CEP'],
            #endereco_pais=CODIGO_BRASIL,
            #endereco_telefone=dados_cliente['telefone'],
        )
        nota_fiscal.cliente = cliente

    # Produto
    nota_fiscal.adicionar_produto_servico(
        codigo='000001',                           # id do produto
        descricao='Box(es) Grammo',
        ncm='21069090',
        cfop='5101',
        unidade_comercial='UN',
        ean='SEM GTIN',
        ean_tributavel='SEM GTIN',
        quantidade_comercial=Decimal('1'),        # X unidades
        valor_unitario_comercial=Decimal(str(dados_compra['valor_total'])),  # preço unitário # noqa
        valor_total_bruto=Decimal(str(dados_compra['valor_total'])),       # preço total # noqa
        unidade_tributavel='UN',
        quantidade_tributavel=Decimal('1'),
        valor_unitario_tributavel=Decimal(str(dados_compra['valor_total'])),
        ind_total=1,
        icms_modalidade='102',
        icms_origem=0,
        icms_csosn='102',
        pis_modalidade='49',
        cofins_modalidade='49',
        valor_tributos_aprox=Decimal('0')
        )

    # responsável técnico
    nota_fiscal.adicionar_responsavel_tecnico(
        cnpj='41602084000110',
        contato='TI Grammo',
        email='ti@grammo.com.br',
        fone='51995601222'
      )

    # serialização
    serializador = SerializacaoXML(_fonte_dados, homologacao=homologacao)
    nfce = serializador.exportar()

    # assinatura
    a1 = AssinaturaA1(certificado, senha)
    xml = a1.assinar(nfce)

    # token de homologacao
    token = '1'

    # csc de homologação
    csc = Config.CSC_homologacao

    # gera e adiciona o qrcode no xml NT2015/003
    xml_com_qrcode = SerializacaoQrcode().gerar_qrcode(token, csc, xml)

    # envio
    con = ComunicacaoSefaz(uf, certificado, senha, homologacao)
    envio = con.autorizacao(modelo='nfce', nota_fiscal=xml_com_qrcode)

    # em caso de sucesso o retorno será o xml autorizado
    # Ps: no modo sincrono, o retorno será o xml completo
    # (<nfeProc> = <NFe> + <protNFe>)
    # no modo async é preciso montar o nfeProc, juntando o retorno com a NFe
    from lxml import etree
    if envio[0] == 0:
        print('NF enviada com sucesso!')
        return etree.tostring(
            envio[1], encoding="unicode").replace('\n', '').replace(
                'ns0:', ''
            )
    # em caso de erro o retorno será o xml de resposta da SEFAZ + NF-e enviada
    else:
        print('Erro:')
        print(envio[1].text)  # resposta
        print('Nota:')
        print(etree.tostring(envio[2], encoding="unicode"))  # nfe


def NF_end_split(end):
    end_dict = {}
    end_list = end.split(',')
    end_dict['logradouro'] = end_list[0].strip()
    tmp = end_list[1].split('-')
    end_dict['numero'] = tmp[0].strip()
    end_dict['bairro'] = tmp[-1].strip()
    tmp2 = end_list[2].split('-')
    end_dict['municipio'] = tmp2[0].strip()
    end_dict['UF'] = tmp2[1].strip()
    end_dict['CEP'] = end_list[3].strip()

    return end_dict


def NF_send(compra_num, cliente=False):

    # Test if the "compra" already have a "NF", end script if true
    if client.grammo.nf.find_one({'compra_num': compra_num}):
        return 'Erro: Compra com NF já emitida'

    # New number for the "NF"
    last_nf_num = list(
        client.grammo.nf.aggregate([
            {"$sort": {"_id": -1}},
            ]))[0]['_id']
    nf_num = last_nf_num + 1

    # Grab the sumary of the "Compra"
    resumo_compra = get_compra_by_num(compra_num)

    # Create a dict with the summary of the "Compra"
    dados_compra = {}
    dados_compra['nf_num'] = nf_num
    dados_compra['valor_total'] = round(
        resumo_compra['bubble']['valor total'], 2
        )

    # Grab details of "Compra" (User info, address, etc) if True
    dados_cliente = {}
    if cliente:
        detalhe_compra = compra_lookup(resumo_compra['bubble']['_id'])
        
        if (len(detalhe_compra[0]['conteúdo_pedido'][0]['end']) > 0):
            end_dict = NF_end_split(
                detalhe_compra[0]['conteúdo_pedido'][0]['end'][0]['endereços']['geo']
                )
            dados_cliente['logradouro'] = end_dict['logradouro']
            dados_cliente['numero'] = end_dict['numero']
            dados_cliente['bairro'] = end_dict['bairro']
            dados_cliente['municipio'] = end_dict['municipio']
            dados_cliente['UF'] = end_dict['UF']
            dados_cliente['CEP'] = end_dict['CEP']
            dados_cliente['complemento'] = detalhe_compra[0]['conteúdo_pedido'][0]['end'][0]['endereços']['complemento'][0:60]
        else:
            dados_cliente['logradouro'] = ''
            dados_cliente['numero'] = ''
            dados_cliente['bairro'] = ''
            dados_cliente['municipio'] = ''
            dados_cliente['UF'] = ''
            dados_cliente['CEP'] = ''
            dados_cliente['complemento'] = ''

        detalhe_usr = detalhe_compra[0]['conteúdo_pedido'][0]['usr'][0]
        dados_cliente['nome'] = detalhe_usr['bio']['nome']
        dados_cliente['CPF'] = detalhe_usr['bio']['CPF']
        dados_cliente['email'] = detalhe_usr['contato']['email']
        dados_cliente['telefone'] = detalhe_usr['contato']['fone']
        
    # Try to send to SEFAZ
    try:
        xml_return = NF_grammo(dados_compra, dados_cliente)
    except Exception as e:
        print(e)

    # If sucess, parse the "chave" and save to DB
    else:
        xml_converted = xmltodict.parse(xml_return)
        chave = xml_converted['nfeProc']['NFe']['infNFe']['@Id'][3:47]

        client.grammo.nf.insert_one(
            {
                '_id': nf_num,
                'data': datetime.datetime.now(),
                'compra_num': compra_num,
                'chave': chave,
                'dados_compra': dados_compra,
                'dados_cliente': dados_cliente,
            },
        )
        return(nf_num)


def NF_insert_one(_id, chave, compra_num, nf_num, valor_total):
    client.grammo.nf.insert_one(
        {
            '_id': _id,  # int
            'chave': chave,  # str
            'compra_num': compra_num,  # int
            'dados_compra': {
                'nf_num': nf_num,  # int
                'valor_total': valor_total  # int
                },
        }
    )
